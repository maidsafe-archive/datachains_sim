use rand::{self, thread_rng, Rand, Rng, SeedableRng, XorShiftRng};
use rand::distributions::range::SampleRange;
use std::cell::RefCell;
use std::env;

thread_local! {
    static SEED: [u32; 4] = match env::var("AGE_SEED") {
        Ok(value) => {
            let nums: Vec<u32> = value.split(|c| c == '[' || c == ']' || c == ' ' || c == ',')
                                      .filter_map(|s| s.parse().ok())
                                      .collect();
            assert_eq!(nums.len(), 4, "AGE_SEED {} isn't in the form '[1, 2, 3, 4]'.", value);
            [nums[0], nums[1], nums[2], nums[3]]
        }
        Err(_) => {
            let mut rng = thread_rng();
            [rng.next_u32().wrapping_add(rng.next_u32()),
             rng.next_u32().wrapping_add(rng.next_u32()),
             rng.next_u32().wrapping_add(rng.next_u32()),
             rng.next_u32().wrapping_add(rng.next_u32())]
        }
    };

    static WEAK_RNG: RefCell<XorShiftRng> = RefCell::new(
        SEED.with(|seed| {
            println!("Seed: {:?}", seed);
            XorShiftRng::from_seed(*seed)
        })
    );
}

/// Get the seed used for the random number generator.
#[allow(unused)]
pub fn seed() -> [u32; 4] {
    SEED.with(|seed| *seed)
}

/// Random value from the thread-local weak RNG.
pub fn random<T: Rand>() -> T {
    WEAK_RNG.with(|rng| rng.borrow_mut().gen())
}

/// Random value from a range from the thread-local weak RNG.
pub fn random_range<T: Rand + PartialOrd + SampleRange>(min: T, max: T) -> T {
    WEAK_RNG.with(|rng| rng.borrow_mut().gen_range(min, max))
}

/// Sample values from an iterator.
#[allow(unused)]
pub fn sample<T, I>(iterable: I, amount: usize) -> Vec<T>
where
    I: IntoIterator<Item = T>,
{
    WEAK_RNG.with(|rng| rand::sample(&mut *rng.borrow_mut(), iterable, amount))
}

/// Sample a single value from an iterator.
#[allow(unused)]
pub fn sample_single<T, I>(iterable: I) -> Option<T>
where
    I: IntoIterator<Item = T>,
{
    sample(iterable, 1).pop()
}

/// Shuffle the mutable slice in place.
pub fn shuffle<T>(values: &mut [T]) {
    WEAK_RNG.with(|rng| rng.borrow_mut().shuffle(values))
}
