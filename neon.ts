import { defineConfig } from "@neon/config/v1";

export default defineConfig({
  // Managed Better Auth: the board's users sign in here, and the API reads the
  // user id from their verified token before it seals any query.
  auth: true,
  preview: {
    // Realtime on this branch. Browsers may only connect from the board's origin;
    // servers and scripts that send no Origin header are not restricted by this.
    realtime: { allowedOrigins: ["http://localhost:5173"] },
  },
});
