extern crate futures;
#[macro_use]
extern crate log;
#[macro_use]
extern crate nom;
extern crate tokio_core;
extern crate tokio_io;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::io::{BufReader, Error, ErrorKind};
use std::iter;
use std::rc::Rc;
use std::str;
use std::sync::{Arc, Mutex};
use std::thread;

use futures::Future;
use futures::stream::{self, Stream};
use nom::IResult;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;
use tokio_io::io;

use job_queue::*;
use parser::*;


mod parser;

mod job_queue;

mod pretty_env_logger;

fn main() {
    pretty_env_logger::init().unwrap();

    let addr = "127.0.0.1:8080".parse().unwrap();

    // Create the event loop and TCP listener we'll accept connections on.
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let socket = TcpListener::bind(&addr, &handle).unwrap();
    println!("Listening on: {}", addr);

    // This is a single-threaded server, so we can just use Rc and RefCell to
    // store the map of all connections we know about.
    let connections = Rc::new(RefCell::new(HashMap::new()));

    let job_queue = Arc::new(Mutex::new(JobQueue::new()));

    let srv = socket.incoming().for_each(move |(stream, addr)| {
        println!("New Connection: {}", addr);
        let (reader, writer) = stream.split();

        // Create a channel for our stream, which other sockets will use to
        // send us messages. Then register our address with the stream to send
        // data to us.
        let (tx, rx) = futures::sync::mpsc::unbounded();
        connections.borrow_mut().insert(addr, tx);

        // Define here what we do for the actual I/O. That is, read a bunch of
        // lines from the socket and dispatch them while we also write any lines
        // from other sockets.
        let connections_inner = connections.clone();
        let reader = BufReader::new(reader);

        // Model the read portion of this socket by mapping an infinite
        // iterator to each line off the socket. This "loop" is then
        // terminated with an error once we hit EOF on the socket.
        let iter = stream::iter_ok::<_, Error>(iter::repeat(()));
        let socket_reader = iter.fold(reader, move |reader, _| {
            // Read a line off the socket, failing if we're at EOF
            let line = io::read_until(reader, b'\n', Vec::new());
            let line = line.and_then(move |(reader, vec)| {
                let mut job_queue = job_queue.lock().unwrap();

                let not_found_response = b"NOT_FOUND\r\n";

                match parse_beanstalk_command(&vec[..]) {
                    IResult::Done(_, command) => {
                        debug!("Received command {:?}", command);
//                        Ok((reader, command))
                    },
                    IResult::Incomplete(_) => {
                        debug!("Unable to parse command - incomplete. Trying to read more data.");
                    },
                    IResult::Error(err) => {
                        debug!("Protocol error from client: {:?}", err);
                    }
                };

                Ok((reader, ""))
            });

            // Convert the bytes we read into a string, and then send that
            // string to all other connected clients.
//            let line = line.map(|(reader, vec)| {
//                (reader, String::from_utf8(vec))
//            });
            let connections = connections_inner.clone();
        });

        // Whenever we receive a string on the Receiver, we write it to
        // `WriteHalf<TcpStream>`.
        let socket_writer = rx.fold(writer, |writer, msg:String| {
            let amt = io::write_all(writer, msg.into_bytes());
            let amt = amt.map(|(writer, _)| writer);
            amt.map_err(|_| ())
        });

        // Now that we've got futures representing each half of the socket, we
        // use the `select` combinator to wait for either half to be done to
        // tear down the other. Then we spawn off the result.
        let connections = connections.clone();
        let socket_reader = socket_reader.map_err(|_| ());
        let connection = socket_reader.map(|_| ()).select(socket_writer.map(|_| ()));
        handle.spawn(connection.then(move |_| {
            connections.borrow_mut().remove(&addr);
            println!("Connection {} closed.", addr);
            Ok(())
        }));

        Ok(())
    });

    // execute server
    core.run(srv).unwrap();
}
