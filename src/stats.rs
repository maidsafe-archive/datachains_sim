use log;
use std::fs::File;
use std::io::Write;
use std::path::Path;

pub struct Stats {
    num_rejections: u64,
    samples: Vec<Sample>,
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            num_rejections: 0,
            samples: Vec::new(),
        }
    }

    pub fn record_reject(&mut self) {
        self.num_rejections += 1;
    }

    pub fn record_sample(&mut self, num_nodes: u64, num_sections: u64, num_complete: u64) {
        self.samples.push(Sample {
            num_nodes,
            num_sections,
            num_complete,
        })
    }

    pub fn print_last(&self) {
        if let Some(last) = self.samples.last() {
            println!(
                "Size: {} Sections: {} Complete: {} Rejections: {}",
                log::important(last.num_nodes),
                log::important(last.num_sections),
                log::important(last.num_complete),
                log::important(self.num_rejections)
            )
        }
    }

    pub fn write_samples_to_file<P: AsRef<Path>>(&self, path: P) {
        let path = path.as_ref();

        let mut file = File::create(path).ok().expect(&format!(
            "Couldn't create file {}!",
            path.display()
        ));

        for (i, sample) in self.samples.iter().enumerate() {
            let _ = write!(
                file,
                "{} {} {} {}\n",
                i,
                sample.num_nodes,
                sample.num_sections,
                sample.num_complete
            );
        }
    }
}

struct Sample {
    num_nodes: u64,
    num_sections: u64,
    num_complete: u64,
}
