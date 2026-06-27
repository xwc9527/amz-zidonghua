# start_server.py — 统一启动 API 服务 (端口8081) 带守护自重启
# 用法: python start_server.py [端口号]
# 访问: http://localhost:8081/dashboard.html

import os, sys, subprocess, time

def main():
    port = sys.argv[1] if len(sys.argv) > 1 else "8081"
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env = os.environ.copy()
    env.setdefault("DB_BACKEND", "sqlite")

    print(f"[守护] API 服务守护进程已启动，端口 {port}", flush=True)
    print(f"[守护] 访问 http://localhost:{port}/dashboard.html", flush=True)

    while True:
        try:
            p = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "api_server:app",
                 "--host", "0.0.0.0", "--port", port],
                cwd=base_dir, env=env
            )
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
