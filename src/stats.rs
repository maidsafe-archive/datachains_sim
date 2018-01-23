use log;
use std::fs::File;
use std::io::Write;
use std::path::Path;

pub struct Stats {
    samples: Vec<Sample>,
    total_merges: u64,
    total_splits: u64,
    total_relocations: u64,
    total_rejections: u64,
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            samples: Vec::new(),
            total_merges: 0,
            total_splits: 0,
            total_relocations: 0,
            total_rejections: 0,
        }
    }

    pub fn record(
        &mut self,
        total_nodes: u64,
        total_sections: u64,
        merges: u64,
        splits: u64,
        relocations: u64,
        rejections: u64,
    ) {
        self.total_merges += merges;
        self.total_splits += splits;
        self.total_relocations += relocations;
        self.total_rejections += rejections;

        info!(
            "Nodes: {} \
             Sections: {} \
             Merges: {} \
             Splits: {} \
             Relocations: {} \
             Rejections: {}",
            log::important(total_nodes),
            log::important(total_sections),
            log::important(self.total_merges),
            log::important(self.total_splits),
            log::important(self.total_relocations),
            log::important(self.total_rejections),
        );

        self.samples.push(Sample {
            nodes: total_nodes,
            sections: total_sections,
            merges: self.total_merges,
            splits: self.total_splits,
            relocations: self.total_relocations,
            rejections: self.total_rejections,
        })
    }

    pub fn print_summary(&self, complete: u64) {
        if let Some(last) = self.samples.last() {
            println!(
                "Iterations:  {:>8}\n\
                 Nodes:       {:>8}\n\
                 Sections:    {:>8} (complete: {})\n\
                 Merges:      {:>8}\n\
                 Splits:      {:>8}\n\
                 Relocations: {:>8}\n\
                 Rejections:  {:>8}",
                self.samples.len(),
                last.nodes,
                last.sections,
                complete,
                last.merges,
                last.splits,
                last.relocations,
                last.rejections,
            )
        }
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) {
        let path = path.as_ref();

        let mut file = File::create(path).ok().expect(&format!(
            "Couldn't create file {}!",
            path.display()
        ));

        for (i, sample) in self.samples.iter().enumerate() {
            let _ =
                write!(
                file,
                "{} {} {} {} {} {} {}\n",
                i,
                sample.nodes,
                sample.sections,
                sample.merges,
                sample.splits,
                sample.relocations,
                sample.rejections,
            );
        }
    }
}

struct Sample {
    nodes: u64,
    sections: u64,
    merges: u64,
    splits: u64,
    relocations: u64,
    rejections: u64,
}
