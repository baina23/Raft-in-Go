# Raft simulator with Golang
This project is a simulator for the Raft consensus protocol. It uses HTTP for inter-server communication, and a [job scheduler](https://github.com/go-co-op/gocron) for implementing polling logic.

## Requirements:
- go 1.17+
- github.com/go-co-op/gocron 1.9.0+


## Concepts
- Actor: The state machine Raft describes. An actor is either a leader, follower, or candidate at a time. Each actor maintains a list of logs.

- Server: Actor + communication methods.

## How to run this project
- First, run `go build` to build the project. GO will automatically download package requirements for you. If it does not, run `go get github.com/go-co-op/gocron` to install the dependency.
- Running with config files:
    - If you wish to provide the server with some initial states, you can create a config file (see json files under [actor_config](./actor_config) for examples) and run `./main -f <config file name>`
        - Note that the log of term 0 is required to avoid index out of bound problems
    - Or you can start a server with no logs at term 0. Run `./main -p <port number> -n <#peers> <peers...>` E.g.: `./main -p 8080 -n 2 8081 8082`
        - `port_number`: the port number you wish to have this server listen on for http traffic. E.g.: 8080
        - `#peers`: the number of peers you want to have in your deployment (excluding this one). E.g.: If you have 3 servers running, this number should be 2 for each of them
        - `peers`: list of port numbers for this server to send RPCs to. E.g.: If this server is on port 8080 and you have two other servers on 8081 and 8082, correspondingly, you should put `8081 8082` here

In order to be able to read the output logs for each server, we recommend to open a separate terminal window for each server.

## How to append log to leader
Use postman or curl to send an http post request to `http://localhost:<leader port number>/client-api` with the following json-formatted payload:
```JSON
{
    "req": "example command string"
}
```