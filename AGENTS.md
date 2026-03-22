IMPORTANT: this is a hobby project that will exclusively be used by 1 user on their personal laptop. Keep this in mind when developing features, they should be simple and not worry about security or migration or backwards compatibility. Assume this app will always run on a local laptop and always needs to be the latest version

NEVER EVER WRITE A DATABASE MIGRATION: just update the database shape and import the data via db:push, I never want to see any migration files in this repo, there is never a backwards compatibility concern.

Never ever write a fallback or silently fail: it's ok to throw errors and when systems or services that are supposed to work do not. DO NOT WRITE ANY FALLBACK

# Broad guidelines

This is an information dense, concise but comprehensive tool to analyze all sorts of information about 1 person: medical information such as bloodwork, food tracking, supplement tracking,workouts and the like

# Stack

- Tooling: Bun + TypeScript
- Server: Bun.serve API
- Client: React + Vite + TanStack Router
- UI: Ant Design + Emotion + plain CSS modules/files. Do not use Tailwind.
- Linting and Hooks: Oxlint + Lefthook
