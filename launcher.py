"""
Keeps the bot running across updates. start-windows.bat / start-mac.command run this instead
of app.py. When the bot finds a newer version on GitHub it saves everything and exits with
code 3; this pulls the update, refreshes packages and starts it again (resuming live mode).
Any other exit (you closed it, Ctrl+C, a crash) ends here as before.
"""
import os
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
EXIT_UPDATE = 3


def update():
    if shutil.which("git") and os.path.isdir(os.path.join(HERE, ".git")):
        print("Installing update...")
        if subprocess.call(["git", "pull", "--ff-only", "--quiet"], cwd=HERE) != 0:
            print("Could not update - starting the current version.")
    subprocess.call([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"], cwd=HERE)


def main():
    os.environ["MOMENTUM_LAUNCHER"] = "1"
    extra = sys.argv[1:]
    while True:
        try:
            code = subprocess.call([sys.executable, "app.py", *extra], cwd=HERE)
        except KeyboardInterrupt:
            return 0
        if code != EXIT_UPDATE:
            return code
        print("\nUpdate found - installing it and restarting...\n")
        time.sleep(2)
        update()
        extra = [a for a in extra if a != "--no-browser"] + ["--no-browser"]   # the dashboard tab is already open


if __name__ == "__main__":
    sys.exit(main())
