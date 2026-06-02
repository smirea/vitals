IMPORTANT: this is a hobby project that will exclusively be used by 1 user on their personal laptop. Keep this in mind when developing features, they should be simple and not worry about security or migration or backwards compatibility. Assume this app will always run on a local laptop and always needs to be the latest version

NEVER EVER WRITE A DATABASE MIGRATION: just update the database shape and import the data via db:push, I never want to see any migration files in this repo, there is never a backwards compatibility concern.

Never ever write a fallback or silently fail: it's ok to throw errors and when systems or services that are supposed to work do not. DO NOT WRITE ANY FALLBACK

All the environment variables are in `.env` and `.env.local` and are managed with the global `env-manager` utility

S3 asset paths include `AWS_PREFIX` after the app root: `s3://<bucket>/vitals/<AWS_PREFIX>/<table>/...`. Use `dev` for local/new writes unless explicitly testing or moving production-backed assets.

# Broad guidelines

This is an information dense, concise but comprehensive tool to analyze all sorts of information about 1 person: medical information such as labs, food tracking, supplement tracking,workouts and the like

# Stack

- Tooling: Bun + TypeScript
- Server: Bun.serve API
- Client: React + Vite + TanStack Router
- UI: Ant Design + Emotion + plain CSS modules/files. Do not use Tailwind.
- Linting and Hooks: Oxlint + Lefthook
