fn main() {
    if let Err(err) = rnon::run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
