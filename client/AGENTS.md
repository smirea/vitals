# Principle

Prefer colocation and simplicity. Keep code as close as possible to where it is used.

# Default Structure

Routes are the default unit of organization.
The page component should live in the route file.
Small helper components and helper functions should stay in that same file when it remains readable.

Shared code should only be extracted when it is actually shared.

# When To Split

If a route gets too large, give that route its own folder and extract only the minimum number of route-local support files.
Use `_`-prefixed files for non-route helpers that should stay colocated but be ignored by TanStack Router.

A good split is:
- route file for the page entry
- one route-local data/model file if needed
- one route-local components file if needed
- route-local CSS if needed

# What Not To Do

Do not extract separate page component files just to avoid a long route file.
Do not split small helpers into many tiny files unless it meaningfully improves readability.

It is fine to have one large file with many small helpers. Only split when the split clearly makes the route easier to understand.
