// mp2-cd: pull 방식 배포기 (docs/05). 5분 cron 으로 돌며 GHCR 의 수집기 이미지 digest 가 바뀌면
// MODE=notify 에서는 Slack 에 "새 이미지 감지" 만, MODE=deploy 에서는 compose pull + up(수집기 4개만) → 75초 뒤 검증 → 실패 시 이전 이미지로 롤백.
//
// 설정: ~/mp2-secrets/cd.env (KEY=VALUE 줄):
//
//	MODE=notify|deploy  SLACK_WEBHOOK_URL=...  IMAGE=ghcr.io/aguantar/market-pipeline-v2-collector  TAG=latest
//	COMPOSE_FILE=/home/calme/market-pipeline-v2/docker-compose.yml  SERVICES=upbit-trades,upbit-orderbook,binance-trades,binance-depth
//	STATE=/home/calme/market-pipeline-v2/data/logs/cd-state.json  VERIFY_SEC=75
//
// 표준 라이브러리만 쓴다. 외부 모듈 없음.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type cfg struct {
	Mode, Slack, Image, Tag, Compose, State string
	Services                                []string
	VerifySec                               int
}

type state struct {
	LastDigest     string    `json:"last_digest"`     // 레지스트리에서 마지막으로 본 digest
	NotifiedDigest string    `json:"notified_digest"` // 알림을 보낸 digest (중복 알림 방지)
	DeployedDigest string    `json:"deployed_digest"` // 마지막 성공 배포 digest
	DeployedAt     time.Time `json:"deployed_at"`
	CheckedAt      time.Time `json:"checked_at"`
}

func loadCfg(path string) (cfg, error) {
	c := cfg{Mode: "notify", Tag: "latest", VerifySec: 75}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		v = strings.Trim(strings.TrimSpace(v), `"`)
		switch strings.TrimSpace(k) {
		case "MODE":
			c.Mode = v
		case "SLACK_WEBHOOK_URL":
			c.Slack = v
		case "IMAGE":
			c.Image = v
		case "TAG":
			c.Tag = v
		case "COMPOSE_FILE":
			c.Compose = v
		case "SERVICES":
			c.Services = strings.Split(v, ",")
		case "STATE":
			c.State = v
		case "VERIFY_SEC":
			fmt.Sscanf(v, "%d", &c.VerifySec)
		}
	}
	if c.Image == "" || c.Compose == "" || c.State == "" || len(c.Services) == 0 {
		return c, errors.New("cd.env: IMAGE, COMPOSE_FILE, STATE, SERVICES 는 필수")
	}
	return c, nil
}

func loadState(path string) state {
	var s state
	b, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(b, &s)
	}
	return s
}

func saveState(path string, s state) error {
	b, _ := json.MarshalIndent(s, "", "  ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// remoteDigest: GHCR 익명 토큰 → HEAD manifests/<tag> → Docker-Content-Digest. 이미지를 내려받지 않는다.
func remoteDigest(image, tag string) (string, error) {
	host, repo, ok := strings.Cut(image, "/")
	if !ok {
		return "", fmt.Errorf("image 형식: %s", image)
	}
	cl := &http.Client{Timeout: 20 * time.Second}
	resp, err := cl.Get(fmt.Sprintf("https://%s/token?scope=repository:%s:pull", host, repo))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var tok struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil || tok.Token == "" {
		return "", fmt.Errorf("token 실패: %v", err)
	}
	req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, repo, tag), nil)
	req.Header.Set("Authorization", "Bearer "+tok.Token)
	req.Header.Set("Accept", "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json")
	resp2, err := cl.Do(req)
	if err != nil {
		return "", err
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		return "", fmt.Errorf("manifest HEAD %d", resp2.StatusCode)
	}
	d := resp2.Header.Get("Docker-Content-Digest")
	if d == "" {
		return "", errors.New("Docker-Content-Digest 없음")
	}
	return d, nil
}

func run(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// runningImageID: 첫 수집기 컨테이너의 이미지 ID (롤백용). 컨테이너 이름은 compose 의 container_name 규약 mp2-<service>.
func runningImageID(service string) string {
	out, err := run("docker", "inspect", "-f", "{{.Image}}", "mp2-"+service)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func slack(webhook, text string) {
	if webhook == "" {
		fmt.Println("[slack 미설정]", text)
		return
	}
	b, _ := json.Marshal(map[string]string{"text": text})
	cl := &http.Client{Timeout: 10 * time.Second}
	resp, err := cl.Post(webhook, "application/json", bytes.NewReader(b))
	if err != nil {
		fmt.Println("slack 실패:", err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		fmt.Println("slack HTTP", resp.StatusCode)
	}
}

func short(d string) string {
	if i := strings.Index(d, ":"); i > 0 && len(d) > i+13 {
		return d[i+1 : i+13]
	}
	return d
}

// verify: 각 수집기가 Running 이고, 재기동 뒤 로그에 [STATS] 가 찍히며 inserted>0 인지. 첫 STATS 까지의 초를 공백 측정값으로 돌려준다.
func verify(services []string, since time.Time, wait int) (map[string]float64, error) {
	time.Sleep(time.Duration(wait) * time.Second)
	gaps := map[string]float64{}
	for _, s := range services {
		name := "mp2-" + s
		st, err := run("docker", "inspect", "-f", "{{.State.Running}} {{.State.StartedAt}}", name)
		if err != nil || !strings.HasPrefix(st, "true") {
			return gaps, fmt.Errorf("%s not running: %s", name, strings.TrimSpace(st))
		}
		logs, _ := run("docker", "logs", "--since", since.UTC().Format(time.RFC3339), name)
		var first time.Time
		ok := false
		for _, ln := range strings.Split(logs, "\n") {
			if !strings.Contains(ln, "[STATS]") {
				continue
			}
			if first.IsZero() && len(ln) >= 19 {
				first, _ = time.Parse("2006-01-02 15:04:05", ln[:19])
			}
			if strings.Contains(ln, "ch inserted=") && !strings.Contains(ln, "ch inserted=0 ") {
				ok = true
			}
		}
		if !ok {
			return gaps, fmt.Errorf("%s: 재기동 뒤 inserted>0 인 STATS 없음", name)
		}
		if !first.IsZero() {
			gaps[s] = first.Sub(since).Seconds()
		}
	}
	return gaps, nil
}

func deploy(c cfg, newDigest string) (string, error) {
	prevID := runningImageID(c.Services[0])
	t0 := time.Now()
	base := []string{"compose", "-f", c.Compose}
	if out, err := run("docker", append(base, append([]string{"pull", "-q"}, c.Services...)...)...); err != nil {
		return "", fmt.Errorf("pull 실패: %v %s", err, out)
	}
	upAt := time.Now()
	if out, err := run("docker", append(base, append([]string{"up", "-d", "--no-build", "--no-deps"}, c.Services...)...)...); err != nil {
		return "", fmt.Errorf("up 실패: %v %s", err, out)
	}
	gaps, err := verify(c.Services, upAt, c.VerifySec)
	if err != nil {
		// 롤백: 이전 이미지 ID 를 같은 태그로 되돌리고 다시 up
		msg := fmt.Sprintf("검증 실패: %v", err)
		if prevID != "" {
			run("docker", "tag", prevID, c.Image+":"+c.Tag)
			run("docker", append(base, append([]string{"up", "-d", "--no-build", "--no-deps"}, c.Services...)...)...)
			msg += fmt.Sprintf(" → 이전 이미지(%s)로 롤백", prevID[7:19])
		}
		return "", errors.New(msg)
	}
	parts := []string{}
	for _, s := range c.Services {
		parts = append(parts, fmt.Sprintf("%s %.0fs", s, gaps[s]))
	}
	return fmt.Sprintf("pull+up %.0fs, 첫 STATS 까지(공백 상한) %s, 전체 %.0fs", upAt.Sub(t0).Seconds(), strings.Join(parts, " / "), time.Since(t0).Seconds()), nil
}

func main() {
	home, _ := os.UserHomeDir()
	cfgPath := filepath.Join(home, "mp2-secrets", "cd.env")
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}
	c, err := loadCfg(cfgPath)
	if err != nil {
		fmt.Println("config:", err)
		os.Exit(2)
	}
	// 겹침 방지 (5분 cron 안에 배포가 끝나지 않을 때)
	lock, err := os.OpenFile(c.State+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err == nil {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			fmt.Println("이전 실행이 아직 진행 중 - 건너뜀")
			return
		}
		defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	}
	st := loadState(c.State)
	now := time.Now().UTC()
	d, err := remoteDigest(c.Image, c.Tag)
	if err != nil {
		fmt.Printf("%s digest 조회 실패: %v\n", now.Format(time.RFC3339), err)
		os.Exit(1)
	}
	st.CheckedAt = now
	first := st.LastDigest == ""
	changed := d != st.LastDigest
	st.LastDigest = d
	switch {
	case first:
		slack(c.Slack, fmt.Sprintf("[mp2-cd] 기준 digest 설정 %s (mode=%s). 다음 변경부터 알림", short(d), c.Mode))
		st.NotifiedDigest = d
		fmt.Printf("%s baseline %s\n", now.Format(time.RFC3339), short(d))
	case changed && c.Mode == "deploy":
		res, err := deploy(c, d)
		if err != nil {
			slack(c.Slack, fmt.Sprintf("[mp2-cd] 배포 실패 %s: %v", short(d), err))
			fmt.Printf("%s deploy FAILED %s: %v\n", now.Format(time.RFC3339), short(d), err)
			st.NotifiedDigest = d
			saveState(c.State, st)
			os.Exit(1)
		}
		st.DeployedDigest, st.DeployedAt, st.NotifiedDigest = d, time.Now().UTC(), d
		slack(c.Slack, fmt.Sprintf("[mp2-cd] 수집기 4개 배포 완료 %s · %s", short(d), res))
		fmt.Printf("%s deployed %s: %s\n", now.Format(time.RFC3339), short(d), res)
	case changed && st.NotifiedDigest != d:
		slack(c.Slack, fmt.Sprintf("[mp2-cd] 새 이미지 감지 %s (mode=notify, 배포 안 함)", short(d)))
		st.NotifiedDigest = d
		fmt.Printf("%s detected %s (notify only)\n", now.Format(time.RFC3339), short(d))
	default:
		fmt.Printf("%s no change %s\n", now.Format(time.RFC3339), short(d))
	}
	if err := saveState(c.State, st); err != nil {
		fmt.Println("state 저장 실패:", err)
	}
}
