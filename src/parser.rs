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
    put_command | reserve_command | delete_command | release_command
));

named!(put_command <Command>, do_parse!(
    tag!("put ") >>
    data: alphanumeric >>
    tag!("\r\n") >>
    (Command::Put {data: data})
));

named!(reserve_command <Command>, do_parse!(
    tag!("reserve\r\n") >>
    (Command::Reserve {})
));

named!(delete_command <Command>, do_parse!(
    tag!("delete ") >>
    id: digit >>
    tag!("\r\n") >>
    (Command::Delete {id: id})
));

named!(release_command <Command>, do_parse!(
    tag!("release ") >>
    id: digit >>
    tag!(" ") >>
    pri: digit >>
    tag!(" ") >>
    delay: digit >>
    tag!("\r\n") >>
    (Command::Release {id: id, pri: pri, delay: delay})
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

#[derive(Debug, PartialEq)]
pub enum Command<'a> {
    Put {data: &'a [u8]},
    Reserve,
    Delete {id: &'a [u8]},
    Release {id: &'a [u8], pri: &'a [u8], delay: &'a [u8]},
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::{IResult, ErrorKind};

    #[test]
    fn parsing_put_command() {
        assert_eq!(
            beanstalk_command(b"put abc\r\n"),
            IResult::Done(&b""[..], Command::Put {data: &b"abc"[..]})
        );
    }

    #[test]
    fn parsing_reserve_command() {
        assert_eq!(
            beanstalk_command(b"reserve\r\n"),
            IResult::Done(&b""[..], Command::Reserve)
        );
        assert_eq!(
            beanstalk_command(b"reserve a\r\n"),
            IResult::Error(ErrorKind::Alt)
        );
    }

    #[test]
    fn parsing_delete_command_with_numeric_id() {
        assert_eq!(
            beanstalk_command(b"delete 1\r\n"),
            IResult::Done(&b""[..], Command::Delete {id: &b"1"[..]})
        );
        assert_eq!(
            beanstalk_command(b"delete 102\r\n"),
            IResult::Done(&b""[..], Command::Delete {id: &b"102"[..]})
        );
    }

    #[test]
    fn parsing_delete_command_with_non_numeric_id() {
        assert_eq!(beanstalk_command(b"delete aaa\r\n"), IResult::Error(ErrorKind::Alt));
        assert_eq!(beanstalk_command(b"delete 1100 aaa\r\n"), IResult::Error(ErrorKind::Alt));
        assert_eq!(beanstalk_command(b"delete aaa 12\r\n"), IResult::Error(ErrorKind::Alt));
    }
}
