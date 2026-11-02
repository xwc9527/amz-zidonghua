"""Download the pinned local Windows/CUDA translator; never stores credentials."""
from pathlib import Path
import argparse
import hashlib
import json
import urllib.request
import zipfile

MODEL = "Qwen3-4B-Instruct-2507-Q4_K_M.gguf"
FILES = [
    (MODEL, "https://huggingface.co/unsloth/Qwen3-4B-Instruct-2507-GGUF/resolve/a06e946bb6b655725eafa393f4a9745d460374c9/" + MODEL,
     "3605803b982cb64aead44f6c1b2ae36e3acdb41d8e46c8a94c6533bc4c67e597"),
    ("llama-b12032-bin-win-cuda-12.4-x64.zip", "https://github.com/ggml-org/llama.cpp/releases/download/b12032/llama-b12032-bin-win-cuda-12.4-x64.zip",
     "36c7e24ad048cceabf00c1b49d0f0411b954c6a410a7ff4300af8721a94fd00a"),
    ("cudart-llama-bin-win-cuda-12.4-x64.zip", "https://github.com/ggml-org/llama.cpp/releases/download/b12032/cudart-llama-bin-win-cuda-12.4-x64.zip",
     "8c79a9b226de4b3cacfd1f83d24f962d0773be79f1e7b75c6af4ded7e32ae1d6"),
]


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()


def setup(root):
    root.mkdir(parents=True, exist_ok=True)
    manifest = []
    for name, url, expected in FILES:
        target = root / name
        if not target.exists() or digest(target) != expected:
            print("Downloading", name, flush=True)
            partial = root / (name + ".part")
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request, timeout=120) as response, partial.open("wb") as out:
                while chunk := response.read(1024 * 1024):
                    out.write(chunk)
            if digest(partial) != expected:
                raise ValueError("SHA256 mismatch: " + name)
            partial.replace(target)
        if target.suffix == ".zip":
            runtime = root / "llama-b12032"
            runtime.mkdir(exist_ok=True)
            with zipfile.ZipFile(target) as archive:
                if archive.testzip() is not None:
                    raise ValueError("Invalid runtime archive")
                for member in archive.namelist():
                    if not (runtime / member).resolve().is_relative_to(runtime.resolve()):
                        raise ValueError("Archive path escapes runtime directory")
                archive.extractall(runtime)
        manifest.append({"file": name, "url": url, "bytes": target.stat().st_size, "sha256": expected})
        print("Verified", name, flush=True)
    (root / "verified-runtime.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="Persistent directory outside the repository")
    setup(parser.parse_args().root.resolve())
