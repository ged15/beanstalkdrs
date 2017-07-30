use nom::{IResult, space, alphanumeric, digit};
use std::iter;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// The received buffer is valid but needs more data
    Incomplete,
    /// Expected one type of argument and received another
    InvalidArgument,
}

named!(beanstalk_command <&[u8], Command>, alt!(
    put_command | reserve_command | delete_command
));

named!(put_command <Command>, do_parse!(
    tag!("put ") >>
    data: alphanumeric >>
    tag!("\r\n") >>
    (Command::Put {data: data})
));

named!(reserve_command <Command>, do_parse!(
    tag!("reserve") >>
    (Command::Reserve {})
));

named!(delete_command <Command>, do_parse!(
    tag!("delete ") >>
    id: digit >>
    (Command::Delete {id: id})
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
        match beanstalk_command(data) {
            IResult::Incomplete(_) => true,
            _ => false,
        }
    }

    pub fn next(&mut self) -> Result<Command, ParseError> {
        let data = &(&*self.data)[self.position..self.written];
        self.position += data.len();

        match beanstalk_command(data) {
            IResult::Done(i, o) => Ok(o),
            IResult::Incomplete(_) => Err(ParseError::Incomplete),
            IResult::Error(_) => Err(ParseError::InvalidArgument),
        }
    }
}

#[derive(Debug)]
pub enum Command<'a> {
    Put {data: &'a [u8]},
    Reserve,
    Delete {id: &'a [u8]},
}
