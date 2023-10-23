# Use an official Golang runtime as a parent image
FROM golang:1.17

# Set the working directory inside the container
WORKDIR /app

# Copy the Golang script to the container
COPY main.go /app/main.go

# Build the Golang script inside the container
RUN go build -o main

# Expose the port the application will listen on (if needed)
# EXPOSE 8080

# Define the command to run your application
CMD ["./main"]
