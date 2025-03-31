# First stage - use the shared library image
FROM shared-lib:latest as shared

# Second stage - build the service
FROM node:14
WORKDIR /app

# Copy package files
COPY package*.json ./

# Copy and install shared library from first stage
COPY --from=shared /output/shared-1.0.0.tgz ./
RUN npm install shared-1.0.0.tgz

# Install dependencies
RUN npm install

# Copy source code
COPY . .

# Create directory for shared lib
# RUN mkdir -p /app/node_modules/shared

# The shared library will be mounted at runtime

CMD ["npm", "run", "start"]