# Use a Node.js base image
FROM node:18-alpine

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json (if available)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the application code
COPY . .

# Expose a port (if your application has a web server, e.g., for health checks)
# EXPOSE 3000

# Command to run the application
CMD ["npm", "start"]
