//! The console binary. Task 8 wires the real UI; for now the launcher
//! sentence is the whole program.

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("tines-gui: launcher");
    }
}
