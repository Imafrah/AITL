# Stage 1: Build the Vite application
FROM node:18-alpine AS builder

WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install

# Copy source code and build
COPY . .
# Setting NODE_ENV to production isn't strictly necessary for Vite build as 'vite build' handles it, 
# but we explicitly do so VITE_API_BASE_URL won't incorrectly point to OnRender via 'import.meta.env.PROD'
# Actually, the App.jsx expects "" when VITE_API_BASE_URL is not set for proxy to work.
ENV VITE_API_BASE_URL=""
RUN npm run build

# Stage 2: Serve the application with Nginx
FROM nginx:alpine

# Copy custom Nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy build artifacts to Nginx html directory
COPY --from=builder /app/dist /usr/share/nginx/html

# Expose port 80
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
