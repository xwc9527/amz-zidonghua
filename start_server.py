# start_server.py — 统一启动 API 服务 (端口8081) 带守护自重启
# 用法: python start_server.py [端口号]
# 访问: http://localhost:8081/dashboard.html
# 局域网访问: set BIND_HOST=0.0.0.0 && python start_server.py
# 热重载: set DEV_RELOAD=1 && python start_server.py （代码改动后自动重启，无需手动杀进程）
#
# 注意: 原先没有 --reload，"守护自重启" 只在进程崩溃/退出时触发，
# 不会监听源码变化——这就是为什么改完 .py 代码后旧进程仍在跑旧逻辑。

import os, sys, subprocess, time

def main():
    port = sys.argv[1] if len(sys.argv) > 1 else "8081"
    host = os.getenv("BIND_HOST", "127.0.0.1")
    dev_reload = os.getenv("DEV_RELOAD", "0") == "1"
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env = os.environ.copy()
    env.setdefault("DB_BACKEND", "sqlite")

    print(f"[守护] API 服务守护进程已启动，{host}:{port}", flush=True)
    print(f"[守护] 访问 http://127.0.0.1:{port}/dashboard.html", flush=True)
    if dev_reload:
        print("[守护] 热重载已开启：*.py 改动会自动重启 worker", flush=True)
    if host == "0.0.0.0":
        print("[守护] 警告: 已绑定 0.0.0.0，局域网可访问爬虫控制接口", flush=True)

    while True:
        try:
            cmd = [sys.executable, "-m", "uvicorn", "api_server:app",
                   "--host", host, "--port", port]
            if dev_reload:
                cmd += ["--reload", "--reload-dir", base_dir]
            p = subprocess.Popen(cmd, cwd=base_dir, env=env)
            p.wait()
            code = p.returncode
            print(f"[守护] API 服务退出 (code={code})，2秒后自动重启...", flush=True)
            time.sleep(2)
        except KeyboardInterrupt:
            print("\n[守护] 正在停止...", flush=True)
            if p and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
            break

if __name__ == "__main__":
    main()
