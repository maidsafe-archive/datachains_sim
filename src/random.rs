use parse::ParseError;
use rand::{self, Rand, Rng, SeedableRng, XorShiftRng};
use rand::distributions::range::SampleRange;
use std::cell::RefCell;
use std::str::FromStr;

thread_local! {
    static WEAK_RNG: RefCell<XorShiftRng> = RefCell::new(
        XorShiftRng::new_unseeded()
    );
}

#[derive(Clone, Copy, Debug)]
pub struct Seed([u32; 4]);

impl Seed {
    pub fn random() -> Self {
        let mut rng = rand::thread_rng();
        Seed(
            [
                rng.next_u32().wrapping_add(rng.next_u32()),
                rng.next_u32().wrapping_add(rng.next_u32()),
                rng.next_u32().wrapping_add(rng.next_u32()),
                rng.next_u32().wrapping_add(rng.next_u32()),
            ],
        )
    }
}

impl FromStr for Seed {
    type Err = ParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut tokens = input
            .split(|c| c == '[' || c == ']' || c == ' ' || c == ',')
            .filter_map(|s| s.parse().ok());
        let mut result = [0; 4];

        for i in 0..result.len() {
            result[i] = tokens.next().ok_or(ParseError)?;
        }

        Ok(Seed(result))
    }
}

/// Set the seed used for the random number generator.
pub fn reseed(seed: Seed) {
    with_rng(|rng| rng.reseed(seed.0))
}

/// Random value from the thread-local weak RNG.
pub fn random<T: Rand>() -> T {
    with_rng(|rng| rng.gen())
}

/// Random value from a range from the thread-local weak RNG.
pub fn random_range<T: Rand + PartialOrd + SampleRange>(min: T, max: T) -> T {
    with_rng(|rng| rng.gen_range(min, max))
}

/// Sample values from an iterator.
#[allow(unused)]
pub fn sample<T, I>(iterable: I, amount: usize) -> Vec<T>
where
    I: IntoIterator<Item = T>,
{
    with_rng(|rng| rand::sample(rng, iterable, amount))
}

/// Sample a single value from an iterator.
pub fn sample_single<T, I>(iterable: I) -> Option<T>
where
    I: IntoIterator<Item = T>,
{
    sample(iterable, 1).pop()
}

/// Shuffle the mutable slice in place.
pub fn shuffle<T>(values: &mut [T]) {
    with_rng(|rng| rng.shuffle(values))
}

/// Generate random boolean with the given probability that it comes up true.
pub fn gen_bool_with_probability(p: f64) -> bool {
    random::<f64>() <= p
}

fn with_rng<F: FnOnce(&mut XorShiftRng) -> R, R>(f: F) -> R {
    WEAK_RNG.with(|rng| f(&mut *rng.borrow_mut()))
}
