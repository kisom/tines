//! The tines bus daemon: routes envelopes between connected clients on a
//! host IPC socket. Usage: `tines-busd <socket-path>`.

fn main() {
    let socket = match std::env::args().nth(1) {
        Some(s) => s,
        None => {
            eprintln!("usage: tines-busd <socket-path>");
            std::process::exit(2);
        }
    };
    if let Err(e) = tines_bus::daemon::serve(&socket) {
        eprintln!("tines-busd: {e}");
        std::process::exit(1);
    }
}
