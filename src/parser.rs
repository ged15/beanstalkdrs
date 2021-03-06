use nom::{IResult, alphanumeric, digit};
use std::str;

// todo: parser for tube name (max 200 bytes)
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

named!(put_command <Command>, do_parse!(
    tag!("put ") >>
    digit >>
    tag!(" ") >>
    digit >>
    tag!(" ") >>
    digit >>
    tag!(" ") >>
    len: map!(digit, |len| str::from_utf8(len).unwrap().parse::<usize>().unwrap()) >>
    tag!("\r\n") >>
    data: take!(len) >>
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

pub fn parse_beanstalk_command(data: &[u8]) -> IResult<&[u8], Command> {
    debug!("Trying to parse '{}'", str::from_utf8(data).unwrap());
    beanstalk_command(data)
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
    fn parsing_put_command_when_data_contains_new_line() {
        assert_eq!(
            beanstalk_command(b"put 1 10 60 7\r\nlab\r\nas\r\n"),
            IResult::Done(&b""[..], Command::Put {data: &b"lab\r\nas"[..]})
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

//    #[test]
//    fn parsing_more_data_than_fits_in_buffer() {
//        let mut sut = Parser::new();
//
//        assert!(sut.is_incomplete());
//        assert_eq!(sut.written, 0);
//        assert_eq!(sut.next(), Err(ParseError::Incomplete));
//
//        sut.allocate();
//        sut.get_mut()[0..].copy_from_slice(&b"put 1 1 1 4\r\n111"[..]);
//        sut.written += 16;
//
//        assert!(sut.is_incomplete());
//        assert_eq!(sut.next(), Err(ParseError::Incomplete));
//
//        sut.allocate();
//
//        {
//            let mut buff = sut.get_mut();
//            buff[16] = 52;
//            buff[17] = 13;
//            buff[18] = 10;
//        }
//
//        sut.written += 3;
//
//        assert_eq!(sut.is_incomplete(), false);
//        assert_eq!(sut.next(), Ok(Command::Put {data: &b"1114"[..]}));
//    }
}
