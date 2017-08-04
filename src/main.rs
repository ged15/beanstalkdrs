#[macro_use]
extern crate nom;

#[macro_use]
extern crate log;

mod parser;

use parser::*;

mod jobqueue;

use jobqueue::*;

mod pretty_env_logger;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str;
use std::sync::{Arc, Mutex};
use std::thread;
use std::iter;

use nom::IResult;

struct Server {
    stream: TcpStream,
    job_queue: Arc<Mutex<JobQueue>>,
}

impl Server {
    fn new(stream: TcpStream, job_queue: Arc<Mutex<JobQueue>>) -> Server {
        Server {
            stream: stream,
            job_queue: job_queue,
        }
    }

    fn run(&mut self) {
        let mut buffer = vec![];
        let mut written = 0;

        loop {
            let incomplete = match parse_beanstalk_command(&(&*buffer)[0..written]) {
                IResult::Incomplete(_) => true,
                _ => false,
            };

            if incomplete {
                buffer.extend(iter::repeat(0).take(16));

                let pos = written;
                let len = match self.stream.read(&mut buffer[pos..]) {
                    Ok(r) => r,
                    Err(err) => {
                        warn!("Failed reading from client: {:?}", err);
                        break;
                    },
                };
                written += len;

                if len == 0 {
                    debug!("Client closed connection");
                    break;
                }
            }

            match parse_beanstalk_command(&(&*buffer)[0..written]) {
                IResult::Done(_, command) => {
                    debug!("Received command {:?}", command);

                    let mut job_queue = self.job_queue.lock().unwrap();

                    let not_found_response = b"NOT_FOUND\r\n";

                    #[allow(unused_must_use)]
                    match command {
                        Command::Put {data} => {
                            let mut alloc_data = Vec::new();
                            alloc_data.extend_from_slice(data);

                            let id = job_queue.put(1, 1, 1, alloc_data);

                            let response = format!("INSERTED {}\r\n", id);

                            self.stream.write(response.as_bytes());
                        },
                        Command::Reserve => {
                            let (job_id, job_data) = job_queue.reserve();

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

                            match job_queue.delete(&id) {
                                Some(_) => self.stream.write(b"DELETED\r\n"),
                                None => self.stream.write(not_found_response),
                            };
                        },
                        Command::Release { id, .. } => {
                            let id = str::from_utf8(id)
                                .unwrap()
                                .parse::<u8>()
                                .unwrap();

                            match job_queue.release(&id) {
                                Some(_) => self.stream.write(b"RELEASED\r\n"),
                                None => self.stream.write(not_found_response),
                            };
                        },
                        Command::Watch { .. } => {
                            self.stream.write(b"WATCHING 1\r\n");
                        },
                        Command::ListTubes {} => {
                            let tube_list = "default";
                            self.stream.write(format!(
                                "OK {}\r\n{}\r\n",
                                tube_list.len(),
                                tube_list
                            ).as_bytes());
                        },
                        Command::StatsTube { .. } => {
                            match job_queue.stats_tube() {
                                Some(response) => self.stream.write(response.to_string().as_bytes()),
                                None => self.stream.write(not_found_response),
                            };
                        },
                        Command::Use {tube} => {
                            self.stream.write(format!("USING {:?}\r\n", tube).as_bytes());
                        },
                        Command::PeekReady {} => {
                            match job_queue.peek_ready() {
                                Some((id, data)) => {
                                    self.stream.write(format!(
                                        "FOUND {} {}\r\n",
                                        id,
                                        data.len()
                                    ).as_bytes());
                                    self.stream.write(data.as_slice());
                                    self.stream.write(b"\r\n");
                                },
                                None => {
                                    self.stream.write(not_found_response);
                                },
                            };
                        },
                        Command::PeekDelayed {} => {
                            self.stream.write(not_found_response);
                        },
                        Command::PeekBuried {} => {
                            self.stream.write(not_found_response);
                        },
                        Command::StatsJob {id} => {
                            let id = str::from_utf8(id)
                                .unwrap()
                                .parse::<u8>()
                                .unwrap();

                            match job_queue.stats_job(&id) {
                                Some(response) => {
                                    self.stream.write(response.to_string().as_bytes());
                                },
                                None => {
                                    self.stream.write(not_found_response);
                                },
                            };

                        },
                    };
                },
                IResult::Incomplete(_) => {
                    debug!("Unable to parse command - incomplete. Trying to read more data.");
                    continue;
                },
                IResult::Error(err) => {
                    debug!("Protocol error from client: {:?}", err);
                    break;
                }
            };

            buffer = vec![];
            written = 0;
        }
    }
}

fn main() {
    pretty_env_logger::init().unwrap();

    let listener = TcpListener::bind("127.0.0.1:11300").unwrap();

    let job_queue = Arc::new(Mutex::new(JobQueue::new()));

    for stream in listener.incoming() {
        match stream {
            Err(_) => panic!("error listen"),
            Ok(stream) => {
                let job_queue = job_queue.clone();
                thread::spawn(move || {
                    debug!("Client connected");

                    let mut server = Server::new(stream, job_queue);
                    server.run();
                });
            },
        };
    }
}
