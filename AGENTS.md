IMPORTANT: this is a hobby project that will exclusively be used by 1 user on their personal laptop. Keep this in mind when developing features, they should be simple and not worry about security or migration or backwards compatibility. Assume this app will always run on a local laptop and always needs to be the latest version

NEVER EVER WRITE A DATABASE MIGRATION: just update the database shape and import the data, I never want to see any migration files in this repo

see `PROJECT.md` for detailed info. when asked to implement new features, update the project file if needed

# Broad guidelines

This is an information dense, concise but comprehensive tool to analyze medical information such as bloodwork

# Stack

- Tooling: Bun + TypeScript
- Server: Bun.serve API
- Client: React + Vite + TanStack Router
- UI: Ant Design + Emotion + Tailwind CSS (enabled in client/src/index.css)
- Linting and Hooks: Oxlint + Lefthook
