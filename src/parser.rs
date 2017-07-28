use nom::{IResult, space, alphanumeric, digit};
use std::iter;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// The received buffer is valid but needs more data
    Incomplete,
    /// Expected one type of argument and received another
    InvalidArgument,
}

named!(beanstalk_request <&[u8], Request>, alt!(
    put_command | reserve_command | delete_command
));

named!(put_command <Request>, do_parse!(
    tag!("put ") >>
    data: alphanumeric >>
    tag!("\r\n") >>
    (Request {command: Command::Put, data: Some(data)})
));

named!(reserve_command <Request>, do_parse!(
    tag!("reserve") >>
    (Request {command: Command::Reserve, data: None})
));

named!(delete_command <Request>, do_parse!(
    tag!("delete ") >>
    id: digit >>
    (Request {command: Command::Delete, data: Some(id)})
));

pub struct Parser {
    data: Vec<u8>,
    position: usize,
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
        match beanstalk_request(data) {
            IResult::Incomplete(_) => true,
            _ => false,
        }
    }

    pub fn next(&mut self) -> Result<Request, ParseError> {
        let data = &(&*self.data)[self.position..self.written];
        self.position += data.len();

        match beanstalk_request(data) {
            IResult::Done(i, o) => Ok(o),
            IResult::Incomplete(_) => Err(ParseError::Incomplete),
            IResult::Error(_) => Err(ParseError::InvalidArgument),
        }
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
