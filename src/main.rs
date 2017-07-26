#[macro_use]
extern crate nom;

use std::iter;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::str;
use nom::{IResult, space, alphanumeric, multispace};

/// A command argument
#[derive(Debug, Clone)]
pub struct Argument {
    /// The position in the array
    pub pos: usize,
    /// The length in the array
    pub len: usize,
}

/// A protocol parser
#[derive(Debug)]
pub struct ParsedCommand<'a> {
    /// The data itself
    data: &'a [u8],
    /// The arguments location and length
    pub argv: Vec<Argument>,
}

impl<'a> ParsedCommand<'a> {
    /// Creates a new parser with the data and arguments provided
    pub fn new(data: &[u8], argv: Vec<Argument>) -> ParsedCommand {
        return ParsedCommand {
            data: data,
            argv: argv,
        };
    }
}

impl OwnedParsedCommand {
    pub fn new(data: Vec<u8>, argv: Vec<Argument>) -> Self {
        OwnedParsedCommand {
            data: data,
            argv: argv,
        }
    }

    pub fn get_command(&self) -> ParsedCommand {
        ParsedCommand::new(&*self.data, self.argv.clone())
    }
}

#[derive(Debug)]
pub struct OwnedParsedCommand {
    data: Vec<u8>,
    pub argv: Vec<Argument>,
}

/// Error parsing
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

    fn response_string(&self) -> String {
        match *self {
            ParseError::Incomplete => "Incomplete data".to_owned(),
            ParseError::BadProtocol(ref s) => format!("Protocol error: {}", s),
            ParseError::InvalidArgument => "Invalid argument".to_owned(),
        }
    }
}

/// Parses the length of the paramenter in the slice
/// Upon success, it returns a tuple with the length of the argument and the
/// length of the parsed length.
fn parse_int(input: &[u8], len: usize, name: &str) -> Result<(Option<usize>, usize), ParseError> {
    if input.len() == 0 {
        return Err(ParseError::Incomplete);
    }
    let mut i = 0;
    let mut argc = 0;
    let mut argco = None;
    while input[i] as char != '\r' {
        let c = input[i] as char;
        if argc == 0 && c == '-' {
            while input[i] as char != '\r' {
                i += 1;
            }
            argco = None;
            break;
        } else if c < '0' || c > '9' {
            return Err(ParseError::BadProtocol(format!("invalid {} length", name)));
        }
        argc *= 10;
        argc += input[i] as usize - '0' as usize;
        i += 1;
        if i == len {
            return Err(ParseError::Incomplete);
        }
        argco = Some(argc);
    }
    i += 1;
    if i == len {
        return Err(ParseError::Incomplete);
    }
    if input[i] as char != '\n' {
        return Err(ParseError::BadProtocol(format!("expected \\r\\n separator, got \\r{}",
                                                   input[i] as char)));
    }
    return Ok((argco, i + 1));
}

pub fn parse(input: &[u8]) -> Result<(ParsedCommand, usize), ParseError> {
    let mut pos = 0;

    named!(beanstalk_request <&[u8], (&[u8], Option<&[u8]>)>,
        do_parse!(
            command: alt!(tag!("put") | tag!("reserve")) >>
            space >>
            data: opt!(alphanumeric) >>
            (command, data)
        )
    );

    let res = beanstalk_request(input);

    match res {
        IResult::Done(i, o) => Ok((ParsedCommand::new(input, Vec::new()), input.len())),
        IResult::Incomplete(_) => Err(ParseError::Incomplete),
        IResult::Error(_) => Err(ParseError::InvalidArgument),
    }

//    println!("parsed {:?}", res);

//    panic!("aaa");

//    Err(ParseError::Incomplete)

//    Ok((ParsedCommand::new(input, Vec::new()), pos))

//    while input.len() > pos && input[pos] as char == '\r' {
//        if pos + 1 < input.len() {
//            if input[pos + 1] as char != '\n' {
//                return Err(ParseError::BadProtocol(format!("expected \\r\\n separator, got \
//                                                            \\r{}",
//                                                           input[pos + 1] as char)));
//            }
//            pos += 2;
//        } else {
//            return Err(ParseError::Incomplete);
//        }
//    }
//    if pos >= input.len() {
//        return Err(ParseError::Incomplete);
//    }
////    if input[pos] as char != '*' {
////        return Err(ParseError::BadProtocol(format!("expected '*', got '{}'", input[pos] as char)));
////    }
//    pos += 1;
//    let len = input.len();
//    let (argco, intlen) = parse_int(&input[pos..len], len - pos, "multibulk")?;
//    let argc = match argco {
//        Some(i) => i,
//        None => 0,
//    };
//    pos += intlen;
////    if argc > 1024 * 1024 {
////        return Err(ParseError::BadProtocol("invalid multibulk length".to_owned()));
////    }
//    let mut argv = Vec::new();
//    for i in 0..argc {
//        if input.len() == pos {
//            return Err(ParseError::Incomplete);
//        }
//        if input[pos] as char != '$' {
//            return Err(ParseError::BadProtocol(format!("expected '$', got '{}'",
//                                                       input[pos] as char)));
//        }
//        pos += 1;
//        let (argleno, arglenlen) = parse_int(&input[pos..len], len - pos, "bulk")?;
//        let arglen = match argleno {
//            Some(i) => i,
//            None => return Err(ParseError::BadProtocol("invalid bulk length".to_owned())),
//        };
//        if arglen > 512 * 1024 * 1024 {
//            return Err(ParseError::BadProtocol("invalid bulk length".to_owned()));
//        }
//        pos += arglenlen;
//        let arg = Argument {
//            pos: pos,
//            len: arglen,
//        };
//        argv.push(arg);
//        pos += arglen + 2;
//        if pos > len || (pos == len && i != argc - 1) {
//            return Err(ParseError::Incomplete);
//        }
//    }
//    Ok((ParsedCommand::new(input, argv), pos))
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
        match parse(data) {
            Ok(_) => false,
            Err(e) => e.is_incomplete(),
        }
    }

    pub fn next(&mut self) -> Result<ParsedCommand, ParseError> {
        let data = &(&*self.data)[self.position..self.written];
        let (r, len) = try!(parse(data));
        self.position += len;
        Ok(r)
    }
}

enum Command {
    Put,
    Reserve,
}

struct Request {
    command: Command,
    data: Option<String>,
}

fn handle_client(mut stream: TcpStream) -> Request {
    let mut buf = Vec::new();

    stream.read_to_end(&mut buf);

    println!("Received input {}", String::from_utf8_lossy(&buf));

    let cmd = match String::from_utf8(buf) {
        Ok(s) => s,
        Err(_) => panic!("Cannot parse input"),
    };

    let mut comm;
    if cmd.starts_with("put ") {
        comm = Command::Put;
        let mut iter = cmd.split_whitespace();
        iter.next();
        let data = iter.next();
        Request {command: Command::Put, data: Some(data.unwrap().to_string())}
    } else if cmd.starts_with("reserve") {
        Request {command: Command::Reserve, data: Some(String::from("ha"))}
    } else {
        panic!("Bad command")
    }
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

        println!("new job with data {}", data);
    }

    fn reserve(self: &mut Self) -> Job {
        match self.queue.pop() {
            Some(j) => j,
            None => panic!("No more jobs!"),
        }
    }

    fn run(&mut self) {
        let mut parser = Parser::new();

        let mut this_command: Option<OwnedParsedCommand>;
        let mut next_command: Option<OwnedParsedCommand> = None;
        loop {
            // FIXME: is_incomplete parses the command a second time
            if next_command.is_none() && parser.is_incomplete() {
                parser.allocate();
                let len = {
                    let pos = parser.written;
                    let mut buffer = parser.get_mut();

                    // read socket
                    match self.stream.read(&mut buffer[pos..]) {
                        Ok(r) => r,
                        Err(err) => {
                            println!("Reading from client: {:?}", err);
//                            sendlog!(sender, Verbose, "Reading from client: {:?}", err);
                            break;
                        }
                    }
                };
                parser.written += len;

                // client closed connection
                if len == 0 {
                    println!("Client closed connection");
//                    sendlog!(sender, Verbose, "Client closed connection");
                    break;
                }
            }

            // was there an error during the execution?
            let mut error = false;

            this_command = next_command;
            next_command = None;

            // try to parse received command
            let parsed_command = match this_command {
                Some(ref c) => c.get_command(),
                None => {
                    match parser.next() {
                        Ok(p) => p,
                        Err(err) => {
                            match err {
                                // if it's incomplete, keep adding to the buffer
                                ParseError::Incomplete => {
                                    println!("Incomplete");
                                    continue;
                                }
                                ParseError::BadProtocol(s) => {
                                    println!("Bad protocol {:?}", s);
//                                    let _ = stream_tx.send(Some(Response::Error(s)));
                                    break;
                                }
                                _ => {
                                    println!("Protocol error from client: {:?}", err);
//                                    sendlog!(sender,
//                                             Verbose,
//                                             "Protocol error from client: {:?}",
//                                             err);
                                    break;
                                }
                            }
                        }
                    }
                }
            };

            println!("{:?}", parsed_command);

//            parsed_command
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
