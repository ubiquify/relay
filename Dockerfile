FROM node:16-alpine
WORKDIR /app
COPY src ./src
COPY package.json ./
COPY package-lock.json ./
COPY tsconfig.json ./
RUN npm install
RUN npm run build
CMD ["npm", "run", "start"]
EXPOSE 3000
EXPOSE 3003
