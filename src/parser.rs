use nom::{IResult, space, alphanumeric};
use std::iter;

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
        command: alt!(tag!("put") | tag!("reserve") | tag!("delete")) >>
        opt!(space) >>
        data: opt!(alphanumeric) >>
        tag!("\r\n") >>
        (command, data)
    )
);

fn parse_nom(input: &[u8]) -> Result<(Request, usize), ParseError> {
    match beanstalk_request(input) {
        IResult::Done(i, o) => {
            let command = match o.0 {
                b"put" => Command::Put,
                b"reserve" => Command::Reserve,
                b"delete" => Command::Delete,
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
pub enum Command {
    Put,
    Reserve,
    Delete,
}

#[derive(Debug)]
pub struct Request<'a> {
    pub command: Command,
    pub data: Option<&'a [u8]>,
}
