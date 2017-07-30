#[macro_use]
extern crate nom;

mod parser;

use parser::*;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str;

struct Job {
    id: u8,
    priority: u8,
    delay: u8,
    ttr: u8,
    data: Vec<u8>,
}

impl Job {
    fn new(id: u8, data: Vec<u8>) -> Job {
        Job {
            id: id,
            priority: 0,
            delay: 0,
            ttr: 1,
            data: data,
        }
    }
}

struct Server {
    queue: HashMap<u8, Job>,
    reserved_jobs: HashMap<u8, Job>,
    stream: TcpStream,
    auto_increment_index: u8,
}

impl Server {
    fn new(stream: TcpStream) -> Server {
        Server {
            queue: HashMap::new(),
            reserved_jobs: HashMap::new(),
            stream: stream,
            auto_increment_index: 0,
        }
    }

    fn put(&mut self, pri: u8, delay: u8, ttr: u8, data: Vec<u8>) -> u8 {
        self.auto_increment_index += 1;
        self.queue.insert(self.auto_increment_index, Job::new(self.auto_increment_index, data));

        self.auto_increment_index
    }

    fn reserve(&mut self) -> (u8, Vec<u8>) {
        let key = self.queue.iter()
            .find(|&(_, &_)| true)
            .map(|(key, _)| key.clone());

        match key {
            Some(id) => {
                let job = self.queue.remove(&id).unwrap();

                let ret = (id, job.data.clone());

                self.reserved_jobs.insert(id, Job::new(job.id, job.data.clone()));

                ret
            },
            None => panic!("No more jobs!"),
        }
    }

    fn delete(&mut self, id: &u8) -> Option<Job> {
        println!("Deleting job {}", id);
        self.queue.remove(id)
    }

    fn run(&mut self) {
        let mut parser = Parser::new();

        loop {
            if parser.is_incomplete() {
                parser.allocate();
                let len = {
                    let pos = parser.written;
                    let mut buffer = parser.get_mut();

                    // read socket
                    match self.stream.read(&mut buffer[pos..]) {
                        Ok(r) => r,
                        Err(err) => {
                            println!("Reading from client: {:?}", err);
                            break;
                        }
                    }
                };
                parser.written += len;

                // client closed connection
                if len == 0 {
                    println!("Client closed connection");
                    break;
                }
            }

            match parser.next() {
                Ok(command) => {
                    println!("Received command {:?}", command);

                    match command {
                        Command::Put {data} => {
                            let mut alloc_data = Vec::new();
                            alloc_data.extend_from_slice(data);

                            let id = self.put(1, 1, 1, alloc_data);

                            let response = format!("INSERTED {}\r\n", id);

                            self.stream.write(response.as_bytes());
                        },
                        Command::Reserve => {
                            let (job_id, job_data) = self.reserve();

                            let header = format!("RESERVED {} {}\r\n", job_id, job_data.len());

                            self.stream.write(header.as_bytes());
                            self.stream.write(job_data.as_slice());
                            self.stream.write(b"\r\n");
                        },
                        Command::Delete {id} => {
                            let id = str::from_utf8(id)
                                .unwrap()
                                .parse::<u8>()
                                .unwrap();

                            match self.delete(&id) {
                                Some(_) => self.stream.write(b"DELETED\r\n"),
                                None => self.stream.write(b"NOT FOUND\r\n"),
                            };
                        },
                    };
                },
                Err(err) => {
                    match err {
                        // if it's incomplete, keep adding to the buffer
                        ParseError::Incomplete => {
                            println!("Incomplete");
                            continue;
                        }
                        _ => {
                            println!("Protocol error from client: {:?}", err);
                            break;
                        }
                    }
                }
            };
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:11300").unwrap();

    for stream in listener.incoming() {
        match stream {
            Err(_) => panic!("error listen"),
            Ok(stream) => {
                let mut server = Server::new(stream);
                server.run();
            },
        };
    }
}
