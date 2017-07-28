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
    deleted: bool,
    reserved: bool,
}

struct Server {
    pub queue: HashMap<u8, Job>,
    pub reserved_jobs: HashMap<u8, Job>,
    pub stream: TcpStream,
    pub auto_increment_index: u8,
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
        self.queue.insert(self.auto_increment_index, Job {
            id: self.auto_increment_index,
            priority: pri,
            delay: delay,
            ttr: ttr,
            data: data,
            deleted: false,
            reserved: false,
        });

        self.auto_increment_index
    }

    fn reserve(&mut self) -> (u8, Vec<u8>) {
        let mut items: Vec<(&u8, &mut Job)> = self.queue.iter_mut()
            .filter(|item| !item.1.reserved)
            .take(1)
            .collect();

        match items.pop() {
            Some((id, job)) => {
                job.reserved = true;

                let ret = (*id, job.data.clone());

                self.reserved_jobs.insert(*id, Job {
                    id: job.id,
                    priority: job.priority,
                    delay: job.delay,
                    ttr: job.ttr,
                    data: job.data.clone(),
                    deleted: false,
                    reserved: false,
                });

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
                Ok(request) => {
                    println!("Received request {:?}", request);

                    match request.command {
                        Command::Put => {
                            let mut data = Vec::new();
                            data.extend_from_slice(request.data.unwrap());

                            let id = self.put(1, 1, 1, data);

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
                        Command::Delete => {
                            let id = str::from_utf8(request.data.unwrap())
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
