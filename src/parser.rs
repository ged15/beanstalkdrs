use nom::{IResult, alphanumeric, digit};
use std::iter;
use std::str;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// The received buffer is valid but needs more data
    Incomplete,
    /// Expected one type of argument and received another
    InvalidArgument,
}

// todo: parser for tube name (max 200 chars)
// todo: parser for data
// todo: parser for ID
// todo: return ID as u8

// todo: make errors propagate from parsers
named!(beanstalk_command <&[u8], Command>, alt!(
    put_command |
    reserve_command |
    delete_command |
    release_command |
    watch_command |
    list_tubes_command |
    stats_tube_command |
    use_command |
    peek_ready_command |
    peek_delayed_command |
    peek_buried_command |
    stats_job_command
));

// @todo messes up when command data contains \r\n
named!(put_command <Command>, do_parse!(
    tag!("put ") >>
    digit >>
    tag!(" ") >>
    digit >>
    tag!(" ") >>
    digit >>
    tag!(" ") >>
    digit >>
    tag!("\r\n") >>
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

named!(watch_command <Command>, do_parse!(
    tag!("watch ") >>
    tube: alphanumeric >>
    tag!("\r\n") >>
    (Command::Watch {tube: tube})
));

named!(list_tubes_command <Command>, do_parse!(
    tag!("list-tubes\r\n") >>
    (Command::ListTubes {})
));

named!(stats_tube_command <Command>, do_parse!(
    tag!("stats-tube ") >>
    tube: alphanumeric >>
    tag!("\r\n") >>
    (Command::StatsTube {tube: tube})
));

named!(use_command <Command>, do_parse!(
    tag!("use") >>
    tube: alt!(
        map!(
            do_parse!(opt!(tag!(" ")) >> tag!("\r\n") >> ()),
            |_| "default".as_bytes()
        ) |
        do_parse!(
            tag!(" ") >>
            tube: alphanumeric >>
            tag!("\r\n") >>
            (tube)
        )
    ) >>
    (Command::Use {tube: tube})
));

named!(peek_ready_command <Command>, do_parse!(
    tag!("peek-ready\r\n") >>
    (Command::PeekReady {})
));

named!(peek_delayed_command <Command>, do_parse!(
    tag!("peek-delayed\r\n") >>
    (Command::PeekDelayed {})
));

named!(peek_buried_command <Command>, do_parse!(
    tag!("peek-buried\r\n") >>
    (Command::PeekBuried {})
));

named!(stats_job_command <Command>, do_parse!(
    tag!("stats-job ") >>
    id: digit >>
    tag!("\r\n") >>
    (Command::StatsJob {id: id})
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
            32 // todo: parser only reads until this
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

        println!("Trying to parse data '{}'", str::from_utf8(data).unwrap().trim_right());

        self.position += data.len();

        match beanstalk_command(data) {
            IResult::Done(_, o) => Ok(o),
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
    Watch {tube: &'a [u8]},
    ListTubes {},
    StatsTube {tube: &'a [u8]},
    Use {tube: &'a [u8]},
    PeekReady {},
    PeekDelayed {},
    PeekBuried {},
    StatsJob {id: &'a [u8]},
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::{IResult, ErrorKind};

    #[test]
    fn parsing_put_command() {
        assert_eq!(
            beanstalk_command(b"put 1 10 60 5\r\nlabas\r\n"),
            IResult::Done(&b""[..], Command::Put {data: &b"labas"[..]})
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

    #[test]
    fn parsing_use_command() {
        assert_eq!(
            beanstalk_command(b"use\r\n"),
            IResult::Done(&b""[..], Command::Use {tube: &b"default"[..]})
        );
        assert_eq!(
            beanstalk_command(b"use \r\n"),
            IResult::Done(&b""[..], Command::Use {tube: &b"default"[..]})
        );
        assert_eq!(
            beanstalk_command(b"use tubename\r\n"),
            IResult::Done(&b""[..], Command::Use {tube: &b"tubename"[..]})
        );
    }
}
