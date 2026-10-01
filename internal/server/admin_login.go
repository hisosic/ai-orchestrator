// Package server — sign-in gate for the admin dashboard.
//
// /admin never ships the dashboard to anonymous or non-admin visitors.
// Instead they get this standalone page: Google sign-in (and the password
// form only when break-glass password login is enabled), or a "no access"
// view for signed-in non-admins.
package server

import (
	"html/template"
	"net/http"
)

type adminGateData struct {
	Forbidden     bool   // signed in, but not an admin
	Username      string // for the forbidden view
	Google        bool
	PasswordLogin bool
	Domain        string
	Error         string
}

func writeAdminGate(w http.ResponseWriter, r *http.Request, status int, d adminGateData) {
	d.Google = googleOAuthConfigured()
	d.PasswordLogin = passwordLoginEnabled()
	d.Domain = googleAllowedDomain()
	if d.Error == "" {
		d.Error = r.URL.Query().Get("login_error")
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	adminGateTmpl.Execute(w, d)
}

var adminGateTmpl = template.Must(template.New("gate").Parse(`<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>관리자 로그인 · AI Container Orchestrator</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='7' fill='%230d1424'/%3E%3Cpath d='M16 5l9 5v12l-9 5-9-5V10z' fill='none' stroke='%235aa7ff' stroke-width='2' stroke-linejoin='round'/%3E%3Cpath d='M7 10l9 5 9-5M16 15v12' fill='none' stroke='%235aa7ff' stroke-width='2' stroke-linejoin='round'/%3E%3C/svg%3E">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/variable/pretendardvariable-dynamic-subset.min.css">
<style>
:root {
  --lp-bg:#0e1626; --lp-fg:#e9eefb; --lp-mut:#9aa8c4; --lp-field:#131f33; --lp-line:#28364f; --lp-ph:#7e8daa;
  --lp-acc:#2f80ff; --lp-acc-fg:#ffffff; --lp-acc-soft:rgba(47,128,255,.20); --lp-acc-softfg:#8fb8ff;
  --lp-chk:#2563eb; --lp-bad:#ff6b7a;
  color-scheme:dark;
}
* { box-sizing:border-box; }
html, body { margin:0; height:100%; }
body { background:#0a0f1a; color:var(--lp-fg); font-family:"Pretendard Variable",Pretendard,"Apple SD Gothic Neo","Malgun Gothic",system-ui,sans-serif; font-size:14px; overflow:hidden; }
#login { position:fixed; inset:0; display:flex; overflow:hidden; }
.login-bg { position:absolute; inset:0; overflow:hidden; background:linear-gradient(165deg,#04070f 0%,#070d22 46%,#120a2b 100%); }
.login-glow { position:absolute; inset:0; background:
  radial-gradient(1100px 620px at 78% 86%, rgba(56,132,255,.34), transparent 62%),
  radial-gradient(860px 520px at 97% 40%, rgba(138,92,246,.24), transparent 60%),
  radial-gradient(660px 420px at 58% 104%, rgba(45,212,255,.20), transparent 62%); }
.login-dots { position:absolute; left:-70%; right:-70%; bottom:0; height:150%;
  transform:perspective(900px) rotateX(62deg); transform-origin:50% 100%;
  background-image:radial-gradient(rgba(150,200,255,.9) 1.6px, transparent 2px); background-size:30px 30px;
  -webkit-mask-image:linear-gradient(transparent 0, rgba(0,0,0,.35) 52%, #000 78%, #000 96%, transparent 100%);
  mask-image:linear-gradient(transparent 0, rgba(0,0,0,.35) 52%, #000 78%, #000 96%, transparent 100%); }
.login-wave { position:absolute; left:0; right:0; top:12%; width:100%; height:62%; opacity:.85; pointer-events:none; }
.login-panel { position:relative; z-index:1; width:40%; min-width:500px; height:100%; background:var(--lp-bg); color:var(--lp-fg);
  padding:144px 32px 32px; overflow:auto; display:flex; flex-direction:column; align-items:center; }
.login-brand { width:100%; max-width:360px; text-align:center; margin-bottom:32px; }
.login-mark { display:flex; align-items:center; justify-content:center; gap:10px; margin-bottom:8px; }
.login-mark svg { width:38px; height:38px; flex:0 0 auto; }
.login-name { font-size:20px; font-weight:700; letter-spacing:-.3px; line-height:1.3; }
.login-tag { margin:0; font-size:14px; line-height:1.5; color:var(--lp-mut); }
.login-form { width:100%; max-width:400px; margin-top:24px; }
.lf-row { margin-bottom:16px; }
.lf-lab { display:block; font-size:14px; font-weight:500; margin-bottom:4px; }
.lf-field { position:relative; }
.lf-field input { width:100%; height:46px; padding:12px 16px 12px 40px; background:var(--lp-field); border:1px solid var(--lp-line);
  border-radius:8px; font-size:14px; font-family:inherit; color:var(--lp-fg); }
.lf-field.pw input { padding-right:48px; }
.lf-field input::placeholder { color:var(--lp-ph); opacity:1; }
.lf-field input:focus { outline:none; border-color:var(--lp-acc); box-shadow:0 0 0 3px var(--lp-acc-soft); }
.lf-ico { position:absolute; left:12px; top:0; bottom:0; width:24px; display:flex; align-items:center; justify-content:center; pointer-events:none; color:var(--lp-mut); }
.lf-ico svg, .lf-eye svg { width:20px; height:20px; }
.lf-eye { position:absolute; right:8px; top:50%; transform:translateY(-50%); width:32px; height:32px; padding:0; display:flex; align-items:center; justify-content:center;
  background:transparent; border:none; border-radius:7px; color:var(--lp-mut); cursor:pointer; }
.lf-eye:hover { color:var(--lp-fg); background:var(--lp-line); }
.lf-check { display:flex; align-items:center; gap:8px; margin-bottom:16px; }
.lf-check input { width:20px; height:20px; margin:0; accent-color:var(--lp-chk); cursor:pointer; }
.lf-check label { font-size:14px; cursor:pointer; }
.lf-mid { min-height:96px; display:flex; flex-direction:column; justify-content:center; gap:6px; padding:8px 0; }
.lf-msg { font-size:13px; line-height:1.5; color:var(--lp-bad); overflow-wrap:anywhere; }
.lf-msg:empty { display:none; }
.lf-msg.busy { color:var(--lp-mut); }
.lf-note { font-size:12px; line-height:1.55; color:var(--lp-mut); }
.lf-submit { width:100%; height:56px; padding:0; border:none; border-radius:12px; font-size:14px; font-weight:600; font-family:inherit;
  cursor:pointer; background:var(--lp-acc); color:var(--lp-acc-fg); display:flex; align-items:center; justify-content:center; gap:10px; text-decoration:none; }
.lf-submit:disabled { background:var(--lp-acc-soft); color:var(--lp-acc-softfg); cursor:not-allowed; }
.lf-submit:focus-visible, .lf-google:focus-visible, .lf-link:focus-visible { outline:2px solid var(--lp-acc); outline-offset:3px; }
.lf-gmark { width:28px; height:28px; border-radius:7px; background:#fff; display:flex; align-items:center; justify-content:center; flex:0 0 auto; }
.lf-gmark svg { width:18px; height:18px; }
.lf-or { display:flex; align-items:center; gap:10px; color:var(--lp-mut); font-size:12px; margin:20px 0; }
.lf-or::before, .lf-or::after { content:""; flex:1; height:1px; background:var(--lp-line); }
.lf-alt { margin-top:14px; display:flex; justify-content:center; gap:16px; font-size:13px; }
.lf-link { color:var(--lp-mut); text-decoration:none; background:none; border:none; font:inherit; cursor:pointer; padding:0; }
.lf-link:hover { color:var(--lp-fg); text-decoration:underline; }
.lf-who { width:100%; padding:14px 16px; border:1px solid var(--lp-line); border-radius:8px; background:var(--lp-field); font-size:14px; overflow-wrap:anywhere; }
@media (max-width:860px) {
  #login { align-items:center; justify-content:center; padding:16px; }
  .login-panel { width:100%; min-width:0; max-width:440px; height:auto; max-height:100%; padding:44px 20px 26px; border-radius:18px;
    box-shadow:rgba(0,0,0,.75) 0 24px 60px -24px; }
  .login-form { margin-top:16px; }
  .lf-mid { min-height:56px; }
}
@media (max-height:760px) { .login-panel { padding-top:64px; } }
</style>
</head>
<body>
<div id="login">
  <div class="login-bg" aria-hidden="true">
    <div class="login-glow"></div>
    <div class="login-dots"></div>
    <svg class="login-wave" viewBox="0 0 1200 600" preserveAspectRatio="none" focusable="false">
      <defs>
        <linearGradient id="lgwA" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0" stop-color="#7c5cff" stop-opacity="0"/><stop offset=".35" stop-color="#7c5cff" stop-opacity=".55"/>
          <stop offset=".72" stop-color="#3fa9ff" stop-opacity=".5"/><stop offset="1" stop-color="#3fa9ff" stop-opacity="0"/>
        </linearGradient>
        <linearGradient id="lgwB" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0" stop-color="#2ee6ff" stop-opacity="0"/><stop offset=".5" stop-color="#2ee6ff" stop-opacity=".45"/>
          <stop offset="1" stop-color="#7c5cff" stop-opacity="0"/>
        </linearGradient>
      </defs>
      <path d="M0 250 C 260 200 400 300 640 262 C 880 224 1000 150 1200 190" fill="none" stroke="url(#lgwA)" stroke-width="1.2" opacity=".6"/>
      <path d="M0 330 C 200 250 330 400 520 350 C 720 296 860 180 1200 250" fill="none" stroke="url(#lgwA)" stroke-width="2"/>
      <path d="M0 420 C 220 350 380 470 600 425 C 830 378 980 300 1200 345" fill="none" stroke="url(#lgwB)" stroke-width="1.6"/>
    </svg>
  </div>

  <main class="login-panel">
    <div class="login-brand">
      <div class="login-mark">
        <svg viewBox="0 0 32 32" aria-hidden="true" focusable="false">
          <path d="M16 3.8l10.5 5.9v12.6L16 28.2 5.5 22.3V9.7z" fill="none" stroke="var(--lp-acc)" stroke-width="2.2" stroke-linejoin="round"/>
          <path d="M5.5 9.7L16 15.6l10.5-5.9M16 15.6v12.6" fill="none" stroke="var(--lp-acc)" stroke-width="2.2" stroke-linejoin="round"/>
        </svg>
        <span class="login-name">AI Container Orchestrator</span>
      </div>
      <p class="login-tag">클러스터를 살피고, 서비스를 배포·운영하는 곳</p>
    </div>

    <div class="login-form">
    {{if .Forbidden}}
      <div class="lf-row">
        <span class="lf-lab">로그인한 계정</span>
        <div class="lf-who">{{.Username}}</div>
      </div>
      <div class="lf-mid">
        <div class="lf-msg">이 계정은 관리자 대시보드에 접근할 권한이 없습니다.</div>
        <div class="lf-note">관리자 권한이 필요하면 클러스터 관리자에게 문의하십시오. 내 서비스는 서비스 포털에서 관리할 수 있습니다.</div>
      </div>
      <a class="lf-submit" href="/">서비스 포털로 이동</a>
      <div class="lf-alt"><button type="button" class="lf-link" id="switchAccount">다른 계정으로 로그인</button></div>
    {{else}}
      {{if .PasswordLogin}}
      <form id="pwForm" autocomplete="on" novalidate>
        <div class="lf-row">
          <label class="lf-lab" for="lfUser">아이디</label>
          <div class="lf-field">
            <span class="lf-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="8" r="3.6"/><path d="M4.8 20c.6-3.6 3.6-5.8 7.2-5.8s6.6 2.2 7.2 5.8"/></svg></span>
            <input id="lfUser" name="username" autocomplete="username" placeholder="아이디를 입력해 주십시오" required>
          </div>
        </div>
        <div class="lf-row">
          <label class="lf-lab" for="lfPass">비밀번호</label>
          <div class="lf-field pw">
            <span class="lf-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="8" cy="12" r="3.6"/><path d="M11.6 12H21"/><path d="M17.6 12v3.2"/><path d="M20.4 12v2.2"/></svg></span>
            <input id="lfPass" name="password" type="password" autocomplete="current-password" placeholder="비밀번호를 입력해 주십시오" required>
            <button type="button" class="lf-eye" id="lfEye" aria-label="비밀번호 보기"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M2.4 12S6 5.6 12 5.6 21.6 12 21.6 12 18 18.4 12 18.4 2.4 12 2.4 12z"/><circle cx="12" cy="12" r="3"/></svg></button>
          </div>
        </div>
        <div class="lf-check"><input type="checkbox" id="lfRemember"><label for="lfRemember">아이디 저장</label></div>
        <div class="lf-mid">
          <div class="lf-msg" id="lfMsg">{{.Error}}</div>
          <div class="lf-note">비상용 비밀번호 로그인이 켜져 있습니다. '아이디 저장'은 아이디만 이 브라우저에 남깁니다.</div>
        </div>
        <button class="lf-submit" type="submit" id="lfSubmit">로그인</button>
      </form>
      {{if .Google}}<div class="lf-or">또는</div>{{end}}
      {{end}}

      {{if .Google}}
      {{if not .PasswordLogin}}
      <div class="lf-mid">
        <div class="lf-msg" id="lfMsg">{{.Error}}</div>
        <div class="lf-note">이 대시보드는 관리자만 볼 수 있습니다.{{if .Domain}} 회사 Google 계정(@{{.Domain}})으로 로그인하십시오.{{end}}</div>
        <div class="lf-note">계정이 없거나 권한이 필요하면 클러스터 관리자에게 문의하십시오.</div>
      </div>
      {{end}}
      <a class="lf-submit lf-google" href="/v1/auth/google/login?redirect=/admin">
        <span class="lf-gmark"><svg viewBox="0 0 48 48" aria-hidden="true"><path fill="#EA4335" d="M24 9.5c3.5 0 6.6 1.2 9 3.6l6.7-6.7C35.6 2.6 30.2 0 24 0 14.6 0 6.4 5.4 2.6 13.2l7.8 6.1C12.2 13.6 17.6 9.5 24 9.5z"/><path fill="#4285F4" d="M46.1 24.6c0-1.6-.1-3.1-.4-4.6H24v9.1h12.4c-.5 2.9-2.1 5.3-4.6 7l7.1 5.5c4.2-3.9 6.6-9.6 6.6-16.5z"/><path fill="#FBBC05" d="M10.4 28.7c-.5-1.4-.8-2.9-.8-4.7s.3-3.3.8-4.7l-7.8-6.1C1 16.3 0 20 0 24s1 7.7 2.6 10.8l7.8-6.1z"/><path fill="#34A853" d="M24 48c6.5 0 11.9-2.1 15.9-5.8l-7.1-5.5c-2 1.3-4.5 2.1-8.8 2.1-6.4 0-11.8-4.1-13.7-9.8l-7.8 6.1C6.4 42.6 14.6 48 24 48z"/></svg></span>
        Google 계정으로 로그인
      </a>
      {{end}}

      {{if and (not .Google) (not .PasswordLogin)}}
      <div class="lf-mid"><div class="lf-msg">사용할 수 있는 로그인 방식이 없습니다. 클러스터 관리자에게 문의하십시오.</div></div>
      {{end}}
      <div class="lf-alt"><a class="lf-link" href="/">서비스 포털로 이동</a></div>
    {{end}}
    </div>
  </main>
</div>
<script>
(function(){
  if (location.search.indexOf('login_error=') >= 0) history.replaceState(null, '', location.pathname);
  var sw = document.getElementById('switchAccount');
  if (sw) sw.addEventListener('click', function(){
    fetch('/v1/auth/logout', {method:'POST', credentials:'same-origin'}).finally(function(){
      location.href = '/v1/auth/google/login?redirect=/admin';
    });
  });
  var form = document.getElementById('pwForm');
  if (!form) return;
  var user = document.getElementById('lfUser'), pass = document.getElementById('lfPass');
  var msg = document.getElementById('lfMsg'), btn = document.getElementById('lfSubmit'), rem = document.getElementById('lfRemember');
  try { var saved = localStorage.getItem('adminLoginId'); if (saved) { user.value = saved; rem.checked = true; pass.focus(); } else { user.focus(); } } catch(e) { user.focus(); }
  document.getElementById('lfEye').addEventListener('click', function(){
    var show = pass.type === 'password'; pass.type = show ? 'text' : 'password';
    this.setAttribute('aria-label', show ? '비밀번호 숨기기' : '비밀번호 보기');
  });
  form.addEventListener('submit', function(e){
    e.preventDefault();
    if (!user.value.trim() || !pass.value) { msg.className = 'lf-msg'; msg.textContent = '아이디와 비밀번호를 입력해 주십시오.'; return; }
    btn.disabled = true; msg.className = 'lf-msg busy'; msg.textContent = '확인 중...';
    fetch('/v1/auth/login', {method:'POST', credentials:'same-origin', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({username:user.value.trim(), password:pass.value})})
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (d && d.success) {
          try { if (rem.checked) localStorage.setItem('adminLoginId', user.value.trim()); else localStorage.removeItem('adminLoginId'); } catch(e) {}
          location.reload();
          return;
        }
        msg.className = 'lf-msg'; msg.textContent = (d && d.message) || '로그인에 실패했습니다.'; btn.disabled = false;
      })
      .catch(function(){ msg.className = 'lf-msg'; msg.textContent = '서버에 연결할 수 없습니다.'; btn.disabled = false; });
  });
})();
</script>
</body>
</html>
`))
