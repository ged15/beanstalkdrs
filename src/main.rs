#[macro_use]
extern crate nom;

use std::iter;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::str;
use nom::{IResult, space, alphanumeric};

#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// The received buffer is valid but needs more data
    Incomplete,
    /// The received buffer is invalid
    BadProtocol(String),
    /// Expected one type of argument and received another
    InvalidArgument,
}

impl ParseError {
    pub fn is_incomplete(&self) -> bool {
        match *self {
            ParseError::Incomplete => true,
            _ => false,
        }
    }
}

named!(beanstalk_request <&[u8], (&[u8], Option<&[u8]>)>,
    do_parse!(
        command: alt!(tag!("put") | tag!("reserve")) >>
        opt!(space) >>
        data: opt!(alphanumeric) >>
        (command, data)
    )
);

fn parse_nom(input: &[u8]) -> Result<(Request, usize), ParseError> {
    match beanstalk_request(input) {
        IResult::Done(i, o) => {
            let command = match o.0 {
                b"put" => Command::Put,
                b"reserve" => Command::Reserve,
                _ => panic!("unknown command")
            };
            Ok((
                Request {command: command, data: o.1},
                input.len()
            ))
        },
        IResult::Incomplete(_) => Err(ParseError::Incomplete),
        IResult::Error(_) => Err(ParseError::InvalidArgument),
    }
}

pub struct Parser {
    data: Vec<u8>,
    pub position: usize,
    pub written: usize,
}

impl Parser {
    pub fn new() -> Parser {
        Parser {
            data: vec![],
            position: 0,
            written: 0,
        }
    }

    pub fn allocate(&mut self) {
        if self.position > 0 && self.written == self.position {
            self.written = 0;
            self.position = 0;
        }

        let len = self.data.len();
        let add = if len == 0 {
            16
        } else if self.written * 2 > len {
            len
        } else {
            0
        };

        if add > 0 {
            self.data.extend(iter::repeat(0).take(add));
        }
    }

    pub fn get_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn is_incomplete(&self) -> bool {
        let data = &(&*self.data)[self.position..self.written];
        match parse_nom(data) {
            Ok(_) => false,
            Err(e) => e.is_incomplete(),
        }
    }

    pub fn next(&mut self) -> Result<Request, ParseError> {
        let data = &(&*self.data)[self.position..self.written];
        let (r, len) = try!(parse_nom(data));
        self.position += len;
        Ok(r)
    }
}

#[derive(Debug)]
enum Command {
    Put,
    Reserve,
}

#[derive(Debug)]
pub struct Request<'a> {
    command: Command,
    data: Option<&'a [u8]>,
}

struct Job {
    priority: u8,
    delay: u8,
    ttr: u8,
    data: String,
}

struct Server {
    pub queue: Vec<Job>,
    pub stream: TcpStream,
}

impl Server {
    fn new(stream: TcpStream) -> Server {
        Server {
            queue: Vec::new(),
            stream: stream,
        }
    }

    fn put(self: &mut Self, pri: u8, delay: u8, ttr: u8, data: String) {
        self.queue.push(Job {
            priority: pri,
            delay: delay,
            ttr: ttr,
            data: data.clone(),
        });

//        println!("new job with data {:?}", data);
    }

    fn reserve(self: &mut Self) -> Job {
        match self.queue.pop() {
            Some(j) => j,
            None => panic!("No more jobs!"),
        }
    }

    fn run(&mut self) {
        let mut parser = Parser::new();

        loop {
            // FIXME: is_incomplete parses the command a second time
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
                    match request.command {
                        Command::Put => {
                            self.put(1, 1, 1, "aaa".to_string());
                        },
                        Command::Reserve => {
                            let job = self.reserve();
//                            println!("reserved job {}", job.data);
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
                        ParseError::BadProtocol(s) => {
                            println!("Bad protocol {:?}", s);
                            break;
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
//                let mut write_stream = match stream.try_clone() {
//                    Ok(s) => s,
//                    Err(_) => panic!("Failed to clone stream"),
//                };
//                let request = handle_client(stream);

//                match request.command {
//                    Command::Put => {
//                        server.put(1, 1, 1, request.data.unwrap());
//                    },
//                    Command::Reserve => {
//                        let job = server.reserve();
//                        println!("reserved job {}", job.data);
//                        write_stream.write(job.data.as_bytes());
//                    },
//                };
            },
        };
    }
}
