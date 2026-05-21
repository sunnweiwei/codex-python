from __future__ import annotations

from codex import CodexConfig, CodexSession


def main() -> None:
    session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True))
    result = session.run("Summarize this project in three concise bullets.")
    print(result.final_message)


if __name__ == "__main__":
    main()

