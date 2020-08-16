use {
    std::sync::Once,
    log::{Record, Metadata, LevelFilter}
};

struct Logger;

impl log::Log for Logger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        println!("|{}| {} - {}", record.target(), record.level(), record.args());
    }

    fn flush(&self) {}
}

static LOGGER: Logger = Logger;
static INIT: Once = Once::new();

pub fn init_logger() {
    INIT.call_once(|| {
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Trace))
            .unwrap();
    })
}
