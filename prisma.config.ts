// Prisma 7 configuration file.
// Replaces the `url` / `directUrl` fields that used to live in schema.prisma.
//
// This URL is used by `prisma migrate`, `prisma studio`, and other CLI commands.
// It MUST be the Neon DIRECT endpoint (hostname without "-pooler"):
//   - Migrations acquire advisory locks, which the pooler does not support.
//   - The same URL is reused by our LISTEN client in db.js.
//
// Prisma Client's own queries do NOT use this URL — they use the adapter
// configured in db.js, which points at the pooled endpoint.

import "dotenv/config";
import { defineConfig } from "prisma/config";

export default defineConfig({
  schema: "prisma/schema.prisma",
  migrations: {
    path: "prisma/migrations",
  },
  datasource: {
    url: process.env.DIRECT_URL ?? process.env.DATABASE_URL ?? "",
  },
});
