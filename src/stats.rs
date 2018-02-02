use std::cmp;
use std::fmt;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::u64;

pub struct Distribution {
    pub min: u64,
    pub max: u64,
    pub avg: f64,
}

impl Distribution {
    pub fn new<I>(values: I) -> Self
    where
        I: IntoIterator<Item = u64>,
    {
        let mut values = values.into_iter();

        if let Some(value) = values.next() {
            let mut min = value;
            let mut max = value;
            let mut avg = value;
            let mut num = 1;

            for value in values {
                min = cmp::min(min, value);
                max = cmp::max(max, value);
                avg += value;
                num += 1;
            }

            Distribution {
                min,
                max,
                avg: avg as f64 / num as f64,
            }
        } else {
            Distribution {
                min: 0,
                max: 0,
                avg: 0.0,
            }
        }
    }
}

impl fmt::Debug for Distribution {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{{ min: {}, max: {}, avg: {:.2} }}",
            self.min,
            self.max,
            self.avg
        )
    }
}

impl fmt::Display for Distribution {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt, "Min: {:6}", self.min)?;
        writeln!(fmt, "Max: {:6}", self.max)?;
        writeln!(fmt, "Avg: {:6.2}", self.avg)
    }
}

#[derive(Clone, Copy, Default)]
pub struct Sample {
    iteration: u64,
    nodes: u64,
    sections: u64,
    merges: u64,
    splits: u64,
    relocations: u64,
    rejections: u64,
}

impl fmt::Debug for Sample {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "{{ iteration: {}, \
            nodes: {}, \
            sections: {}, \
            merges: {}, \
            splits: {}, \
            relocations: {} \
            rejections: {} }}",
            self.iteration,
            self.nodes,
            self.sections,
            self.merges,
            self.splits,
            self.relocations,
            self.rejections,
        )
    }
}

impl fmt::Display for Sample {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        writeln!(fmt,
            "Iteration:   {:>8}\n\
             Nodes:       {:>8}\n\
             Sections:    {:>8}\n\
             Merges:      {:>8}\n\
             Splits:      {:>8}\n\
             Relocations: {:>8}\n\
             Rejections:  {:>8}",
            self.iteration,
            self.nodes,
            self.sections,
            self.merges,
            self.splits,
            self.relocations,
            self.rejections,
        )
    }
}

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
        iteration: u64,
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

        self.samples.push(Sample {
            iteration,
            nodes: total_nodes,
            sections: total_sections,
            merges: self.total_merges,
            splits: self.total_splits,
            relocations: self.total_relocations,
            rejections: self.total_rejections,
        })
    }

    pub fn summary(&self) -> Sample {
        self.samples.last().cloned().unwrap_or(Sample::default())
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) {
        let path = path.as_ref();

        let mut file = File::create(path).ok().expect(&format!(
            "Couldn't create file {}!",
            path.display()
        ));

        for sample in &self.samples {
            let _ =
                write!(
                file,
                "{} {} {} {} {} {} {}\n",
                sample.iteration,
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
