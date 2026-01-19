
//! `stringsort`: streaming sort for byte-slice keys (`&[u8]`) using a hybrid of
//! CPS-quicksort (common-prefix skipping quicksort) and MSD radix steps, with a
//! reusable moving cache window per key.
//!
//! Inspired by patent US7680791B2:
//! https://patents.google.com/patent/US7680791B2
//!
//! # Highlights
//! - Streams sorted output into a bounded `SyncSender` (natural backpressure).
//! - Optional `limit` for early exit after sending `limit` keys.
//! - Const-generic cache width `W` (e.g., `sort_stream::<8>(...)`).
//! - Struct-of-arrays (SoA) layout + reusable scratch buffers for radix distribution.
//!
//! # Ordering
//! Lexicographic over bytes. Shorter key sorts before longer when one is a prefix of the other.
//!
//! # Stability
//! Not stable.

use core::cmp::Ordering;
use std::sync::mpsc::{SendError, SyncSender};

/// Sort configuration knobs.
#[derive(Clone, Copy, Debug)]
pub struct SortConfig {
    /// Prefer MSD radix distribution when subproblem length >= this threshold.
    pub radix_threshold: usize,
    /// Use insertion sort when subproblem length <= this threshold.
    pub insertion_threshold: usize,
    /// Safety cap on recursion depth (bytes). If exceeded, fall back to `sort_unstable()`.
    pub max_depth: usize,
    /// For comparisons, require at least this many cached bytes remaining at current depth
    /// before reusing a cache window; otherwise refill at the current depth.
    pub min_remain_for_cmp: usize,
    /// For radix bucketing, require at least this many cached bytes remaining at current depth
    /// before reusing a cache window; otherwise refill at the current depth.
    pub min_remain_for_radix: usize,
}

impl Default for SortConfig {
    fn default() -> Self {
        Self {
            radix_threshold: 128,
            insertion_threshold: 24,
            max_depth: 1 << 20,
            min_remain_for_cmp: 2,
            min_remain_for_radix: 1,
        }
    }
}

/// Streams keys in sorted order into `tx`.
///
/// - `W` is the cache window size in bytes.
/// - `limit`: if `Some(L)`, stop early after sending `L` keys.
///
/// Returns the number of keys actually sent.
/// Returns `Err` if the receiver is dropped (propagated from `SyncSender::send`).
pub fn sort_stream<'a, const W: usize>(
    keys: &[&'a [u8]],
    tx: SyncSender<&'a [u8]>,
    cfg: SortConfig,
    limit: Option<usize>,
) -> Result<usize, SendError<&'a [u8]>> {
    if keys.is_empty() {
        return Ok(0);
    }
    if W == 0 {
        // Degenerate: no caching. Still stream correctly.
        let mut tmp = keys.to_vec();
        tmp.sort_unstable();
        let mut sent = 0usize;
        let lim = limit.unwrap_or(usize::MAX);
        for &k in &tmp {
            if sent >= lim {
                break;
            }
            tx.send(k)?;
            sent += 1;
        }
        return Ok(sent);
    }
    if W > u8::MAX as usize {
        // Keep metadata compact (valid stored as u8).
        // You can relax this by switching `valid` to `u16`.
        let mut tmp = keys.to_vec();
        tmp.sort_unstable();
        let mut sent = 0usize;
        let lim = limit.unwrap_or(usize::MAX);
        for &k in &tmp {
            if sent >= lim {
                break;
            }
            tx.send(k)?;
            sent += 1;
        }
        return Ok(sent);
    }

    let n = keys.len();

    // SoA storage: keys + cache metadata + cache bytes.
    let mut ks: Vec<&'a [u8]> = keys.to_vec();
    let mut base: Vec<usize> = vec![0usize; n];
    let mut valid: Vec<u8> = vec![0u8; n];
    let mut cache: Vec<[u8; W]> = vec![[0u8; W]; n];

    // Initialize cache windows at base=0.
    for i in 0..n {
        refill_cache::<W>(ks[i], 0, &mut base[i], &mut valid[i], &mut cache[i]);
    }

    // Reusable radix scratch buffers (SoA).
    let mut scratch = Scratch::<W>::new(n);

    // Streaming context (limit + sent count).
    let mut ctx = StreamCtx::new(limit);

    // Sort and stream.
    sort_range_stream::<W>(
        ViewMut {
            keys: &mut ks,
            base: &mut base,
            valid: &mut valid,
            cache: &mut cache,
        },
        0,
        &tx,
        &cfg,
        &mut scratch,
        &mut ctx,
    )?;

    Ok(ctx.sent)
}

struct StreamCtx {
    sent: usize,
    limit: usize,
}

impl StreamCtx {
    fn new(limit: Option<usize>) -> Self {
        Self {
            sent: 0,
            limit: limit.unwrap_or(usize::MAX),
        }
    }

    #[inline]
    fn done(&self) -> bool {
        self.sent >= self.limit
    }

    #[inline]
    fn emit<'a>(
        &mut self,
        tx: &SyncSender<&'a [u8]>,
        key: &'a [u8],
    ) -> Result<(), SendError<&'a [u8]>> {
        if self.done() {
            return Ok(());
        }
        tx.send(key)?;
        self.sent += 1;
        Ok(())
    }
}

/// Mutable parallel slices (SoA view).
struct ViewMut<'a, const W: usize> {
    keys: &'a mut [&'a [u8]],
    base: &'a mut [usize],
    valid: &'a mut [u8],
    cache: &'a mut [[u8; W]],
}

impl<'a, const W: usize> ViewMut<'a, W> {
    #[inline]
    fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[inline]
    fn swap(&mut self, i: usize, j: usize) {
        self.keys.swap(i, j);
        self.base.swap(i, j);
        self.valid.swap(i, j);
        self.cache.swap(i, j);
    }

    #[inline]
    fn split_at_mut(self, mid: usize) -> (ViewMut<'a, W>, ViewMut<'a, W>) {
        let (k1, k2) = self.keys.split_at_mut(mid);
        let (b1, b2) = self.base.split_at_mut(mid);
        let (v1, v2) = self.valid.split_at_mut(mid);
        let (c1, c2) = self.cache.split_at_mut(mid);
        (
            ViewMut {
                keys: k1,
                base: b1,
                valid: v1,
                cache: c1,
            },
            ViewMut {
                keys: k2,
                base: b2,
                valid: v2,
                cache: c2,
            },
        )
    }
}

/// Reusable scratch buffers for radix distribution.
struct Scratch<const W: usize> {
    keys: Vec<*const [u8]>, // raw pointers to avoid lifetime headaches in scratch
    base: Vec<usize>,
    valid: Vec<u8>,
    cache: Vec<[u8; W]>,
}

impl<const W: usize> Scratch<W> {
    fn new(cap: usize) -> Self {
        Self {
            keys: vec![core::ptr::null(); cap],
            base: vec![0usize; cap],
            valid: vec![0u8; cap],
            cache: vec![[0u8; W]; cap],
        }
    }

    fn ensure_len(&mut self, n: usize) {
        if self.keys.len() < n {
            self.keys.resize(n, core::ptr::null());
            self.base.resize(n, 0);
            self.valid.resize(n, 0);
            self.cache.resize(n, [0u8; W]);
        }
    }
}

fn sort_range_stream<'a, const W: usize>(
    mut v: ViewMut<'a, W>,
    depth: usize,
    tx: &SyncSender<&'a [u8]>,
    cfg: &SortConfig,
    scratch: &mut Scratch<W>,
    ctx: &mut StreamCtx,
) -> Result<(), SendError<&'a [u8]>> {
    if v.is_empty() || ctx.done() {
        return Ok(());
    }

    if depth >= cfg.max_depth {
        // Safety fallback: sort slice by full key compare, then emit (respecting limit).
        v.keys.sort_unstable();
        for &k in v.keys.iter() {
            if ctx.done() {
                break;
            }
            ctx.emit(tx, k)?;
        }
        return Ok(());
    }

    if v.len() <= cfg.insertion_threshold {
        insertion_sort::<W>(&mut v, depth, cfg);
        for &k in v.keys.iter() {
            if ctx.done() {
                break;
            }
            ctx.emit(tx, k)?;
        }
        return Ok(());
    }

    if v.len() >= cfg.radix_threshold {
        radix_step_stream::<W>(v, depth, tx, cfg, scratch, ctx)?;
        return Ok(());
    }

    cps_quicksort_stream::<W>(v, depth, tx, cfg, scratch, ctx)
}

fn cps_quicksort_stream<'a, const W: usize>(
    mut v: ViewMut<'a, W>,
    depth: usize,
    tx: &SyncSender<&'a [u8]>,
    cfg: &SortConfig,
    scratch: &mut Scratch<W>,
    ctx: &mut StreamCtx,
) -> Result<(), SendError<&'a [u8]>> {
    if v.len() == 0 || ctx.done() {
        return Ok(());
    }

    // Choose pivot and snapshot it (since swaps would move it).
    let p = v.len() / 2;
    ensure_cache_at::<W>(&mut v, p, depth, cfg.min_remain_for_cmp);

    let pivot_key = v.keys[p];
    let pivot_base = v.base[p];
    let pivot_valid = v.valid[p] as usize;
    let pivot_cache = v.cache[p]; // Copy [u8; W]

    // Dutch national flag partition.
    let mut lt = 0usize;
    let mut i = 0usize;
    let mut gt = v.len();

    // Track earliest mismatch positions against pivot in lt/gt partitions.
    let mut lt_cp = usize::MAX;
    let mut gt_cp = usize::MAX;

    while i < gt {
        ensure_cache_at::<W>(&mut v, i, depth, cfg.min_remain_for_cmp);
        let (ord, first_diff) = cmp_with_pivot::<W>(
            v.keys[i],
            v.base[i],
            v.valid[i] as usize,
            &v.cache[i],
            depth,
            pivot_key,
            pivot_base,
            pivot_valid,
            &pivot_cache,
        );

        match ord {
            Ordering::Less => {
                lt_cp = lt_cp.min(first_diff);
                v.swap(lt, i);
                lt += 1;
                i += 1;
            }
            Ordering::Greater => {
                gt -= 1;
                gt_cp = gt_cp.min(first_diff);
                v.swap(i, gt);
            }
            Ordering::Equal => {
                i += 1;
            }
        }
    }

    // Recurse / emit left
    if lt > 0 && !ctx.done() {
        let next_depth = if lt_cp == usize::MAX { depth } else { lt_cp };
        let (left, rest) = v.split_at_mut(lt);
        // `rest` contains eq+gt; we'll split again below without allocating.
        sort_range_stream::<W>(left, next_depth, tx, cfg, scratch, ctx)?;
        v = rest; // continue on remaining region
    }

    if ctx.done() {
        return Ok(());
    }

    // Now v is [eq..gt] because we reassigned v=rest; we need to find its boundaries relative to original.
    // Original partitions: [0..lt) [lt..gt) [gt..n)
    // After split_at_mut(lt), `v` is [lt..n). Its "eq" begins at 0 and ends at (gt-lt).
    let eq_len = gt - lt;
    let (eq, right) = v.split_at_mut(eq_len);

    // Emit equals
    for &k in eq.keys.iter() {
        if ctx.done() {
            break;
        }
        ctx.emit(tx, k)?;
    }

    // Recurse / emit right
    if !right.is_empty() && !ctx.done() {
        let next_depth = if gt_cp == usize::MAX { depth } else { gt_cp };
        sort_range_stream::<W>(right, next_depth, tx, cfg, scratch, ctx)?;
    }

    Ok(())
}

/// MSD radix distribution at `depth`.
///
/// Buckets:
/// - 0: END (key ends at depth)
/// - 1..=256: byte value + 1
fn radix_step_stream<'a, const W: usize>(
    mut v: ViewMut<'a, W>,
    depth: usize,
    tx: &SyncSender<&'a [u8]>,
    cfg: &SortConfig,
    scratch: &mut Scratch<W>,
    ctx: &mut StreamCtx,
) -> Result<(), SendError<&'a [u8]>> {
    if v.is_empty() || ctx.done() {
        return Ok(());
    }

    // Count buckets.
    let mut counts = [0usize; 257];
    for i in 0..v.len() {
        ensure_cache_at::<W>(&mut v, i, depth, cfg.min_remain_for_radix);
        let b = bucket_at::<W>(v.keys[i], v.base[i], v.valid[i] as usize, &v.cache[i], depth);
        counts[b] += 1;
    }

    // Prefix sums -> offsets.
    let mut offsets = [0usize; 257];
    let mut sum = 0usize;
    for i in 0..257 {
        offsets[i] = sum;
        sum += counts[i];
    }

    // Distribute into scratch (stable).
    scratch.ensure_len(v.len());
    let mut next = offsets;

    for i in 0..v.len() {
        let b = bucket_at::<W>(v.keys[i], v.base[i], v.valid[i] as usize, &v.cache[i], depth);
        let pos = next[b];
        next[b] += 1;

        scratch.keys[pos] = v.keys[i] as *const [u8];
        scratch.base[pos] = v.base[i];
        scratch.valid[pos] = v.valid[i];
        scratch.cache[pos] = v.cache[i];
    }

    // Copy back.
    for i in 0..v.len() {
        // Safety: pointers were created from the same slice lifetime `'a`.
        v.keys[i] = unsafe { &*scratch.keys[i] };
        v.base[i] = scratch.base[i];
        v.valid[i] = scratch.valid[i];
        v.cache[i] = scratch.cache[i];
    }

    // Emit / recurse in bucket order.
    let mut start = 0usize;

    // END bucket first.
    let end_len = counts[0];
    for i in start..start + end_len {
        if ctx.done() {
            return Ok(());
        }
        ctx.emit(tx, v.keys[i])?;
    }
    start += end_len;

    // Remaining byte buckets.
    for bi in 1..257 {
        let len = counts[bi];
        if len == 0 {
            continue;
        }
        let (prefix, rest) = v.split_at_mut(start);
        let (bucket, rest2) = rest.split_at_mut(len);
        // `prefix` is ignored; we only needed splitting.
        drop(prefix);

        sort_range_stream::<W>(bucket, depth + 1, tx, cfg, scratch, ctx)?;
        if ctx.done() {
            return Ok(());
        }

        // Continue with remaining tail.
        v = rest2;
        start = 0;
    }

    Ok(())
}

fn insertion_sort<const W: usize>(v: &mut ViewMut<'_, W>, depth: usize, cfg: &SortConfig) {
    for i in 1..v.len() {
        let mut j = i;
        while j > 0 {
            ensure_cache_at::<W>(v, j, depth, cfg.min_remain_for_cmp);
            ensure_cache_at::<W>(v, j - 1, depth, cfg.min_remain_for_cmp);

            let (ord, _) = cmp_entries::<W>(
                v.keys[j - 1],
                v.base[j - 1],
                v.valid[j - 1] as usize,
                &v.cache[j - 1],
                v.keys[j],
                v.base[j],
                v.valid[j] as usize,
                &v.cache[j],
                depth,
            );

            if ord == Ordering::Greater {
                v.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
}

#[inline]
fn ensure_cache_at<const W: usize>(
    v: &mut ViewMut<'_, W>,
    idx: usize,
    depth: usize,
    min_remain: usize,
) {
    let base = v.base[idx];
    let valid = v.valid[idx] as usize;

    if depth < base {
        refill_cache::<W>(v.keys[idx], depth, &mut v.base[idx], &mut v.valid[idx], &mut v.cache[idx]);
        return;
    }
    let off = depth - base;
    let remain = valid.saturating_sub(off);
    if remain < min_remain {
        refill_cache::<W>(v.keys[idx], depth, &mut v.base[idx], &mut v.valid[idx], &mut v.cache[idx]);
    }
}

#[inline]
fn refill_cache<const W: usize>(
    key: &[u8],
    new_base: usize,
    base_out: &mut usize,
    valid_out: &mut u8,
    cache_out: &mut [u8; W],
) {
    *base_out = new_base;
    if new_base >= key.len() {
        *valid_out = 0;
        return;
    }
    let take = (key.len() - new_base).min(W);
    cache_out[..take].copy_from_slice(&key[new_base..new_base + take]);
    if take < W {
        cache_out[take..].fill(0);
    }
    *valid_out = take as u8;
}

#[inline]
fn bucket_at<const W: usize>(
    key: &[u8],
    base: usize,
    valid: usize,
    cache: &[u8; W],
    depth: usize,
) -> usize {
    if depth >= key.len() {
        return 0;
    }
    if depth >= base {
        let off = depth - base;
        if off < valid {
            return (cache[off] as usize) + 1;
        }
    }
    // Fallback to direct key byte (should be rare if cache policy is sane).
    (key[depth] as usize) + 1
}

/// Compare entry against a pivot snapshot (key + cache window metadata).
/// Returns (ordering, first_diff_pos_absolute).
fn cmp_with_pivot<const W: usize>(
    key: &[u8],
    base: usize,
    valid: usize,
    cache: &[u8; W],
    depth: usize,
    pivot_key: &[u8],
    pivot_base: usize,
    pivot_valid: usize,
    pivot_cache: &[u8; W],
) -> (Ordering, usize) {
    // Compare cached overlap starting at depth.
    let (a_off, a_rem) = cache_suffix(base, valid, depth);
    let (b_off, b_rem) = cache_suffix(pivot_base, pivot_valid, depth);
    let common = a_rem.min(b_rem);

    for j in 0..common {
        let aa = cache[a_off + j];
        let bb = pivot_cache[b_off + j];
        if aa != bb {
            return (aa.cmp(&bb), depth + j);
        }
    }

    // Fallback compare beyond common cached prefix.
    let start = depth + common;
    cmp_from_index(key, pivot_key, start)
}

fn cmp_entries<const W: usize>(
    a: &[u8],
    a_base: usize,
    a_valid: usize,
    a_cache: &[u8; W],
    b: &[u8],
    b_base: usize,
    b_valid: usize,
    b_cache: &[u8; W],
    depth: usize,
) -> (Ordering, usize) {
    let (a_off, a_rem) = cache_suffix(a_base, a_valid, depth);
    let (b_off, b_rem) = cache_suffix(b_base, b_valid, depth);
    let common = a_rem.min(b_rem);

    for j in 0..common {
        let aa = a_cache[a_off + j];
        let bb = b_cache[b_off + j];
        if aa != bb {
            return (aa.cmp(&bb), depth + j);
        }
    }

    let start = depth + common;
    cmp_from_index(a, b, start)
}

#[inline]
fn cache_suffix(base: usize, valid: usize, depth: usize) -> (usize, usize) {
    if depth < base {
        return (0, 0);
    }
    let off = depth - base;
    if off >= valid {
        return (0, 0);
    }
    (off, valid - off)
}

fn cmp_from_index(a: &[u8], b: &[u8], start: usize) -> (Ordering, usize) {
    let mut i = start;
    let min_len = a.len().min(b.len());
    while i < min_len {
        let aa = a[i];
        let bb = b[i];
        if aa != bb {
            return (aa.cmp(&bb), i);
        }
        i += 1;
    }
    let ord = a.len().cmp(&b.len());
    (ord, min_len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::sync_channel;

    // Deterministic RNG (no external deps).
    #[derive(Clone)]
    struct XorShift64(u64);
    impl XorShift64 {
        fn new(seed: u64) -> Self {
            Self(seed)
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn next_usize(&mut self, bound: usize) -> usize {
            (self.next_u64() as usize) % bound
        }
        fn next_byte(&mut self) -> u8 {
            (self.next_u64() & 0xFF) as u8
        }
    }

    fn run_sort<const W: usize>(keys: &[&[u8]], limit: Option<usize>) -> Vec<Vec<u8>> {
        let (tx, rx) = sync_channel::<&[u8]>(8);
        let sent = sort_stream::<W>(keys, tx, SortConfig::default(), limit).unwrap();
        let out: Vec<Vec<u8>> = rx.into_iter().map(|k| k.to_vec()).collect();
        assert_eq!(out.len(), sent);
        out
    }

    #[test]
    fn basic_sort() {
        let keys: Vec<&[u8]> = vec![b"b", b"aa", b"a", b"ab", b"", b"a"];
        let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        expected.sort_unstable();

        let out = run_sort::<8>(&keys, None);
        assert_eq!(out, expected);
    }

    #[test]
    fn limit_early_exit() {
        let keys: Vec<&[u8]> = vec![b"b", b"aa", b"a", b"ab", b"", b"a"];
        let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        expected.sort_unstable();
        let expected3 = expected.into_iter().take(3).collect::<Vec<_>>();

        let out = run_sort::<8>(&keys, Some(3));
        assert_eq!(out, expected3);
    }

    #[test]
    fn random_matches_std_full() {
        let mut rng = XorShift64::new(0x1234_5678_9ABC_DEF0);
        let mut owned: Vec<Vec<u8>> = Vec::new();

        for _ in 0..8000 {
            let prefix_len = rng.next_usize(6); // 0..5
            let total_len = prefix_len + rng.next_usize(48); // up to ~53
            let mut v = Vec::with_capacity(total_len);

            // structured prefix
            for _ in 0..prefix_len {
                v.push(b'a' + (rng.next_byte() % 3));
            }
            // random tail
            for _ in prefix_len..total_len {
                v.push(rng.next_byte());
            }
            owned.push(v);
        }

        let keys: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();

        let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        expected.sort_unstable();

        let out = run_sort::<8>(&keys, None);
        assert_eq!(out, expected);
    }

    #[test]
    fn random_matches_std_with_limit() {
        let mut rng = XorShift64::new(0xCAFEBABE_u64);
        let mut owned: Vec<Vec<u8>> = Vec::new();

        for _ in 0..5000 {
            let total_len = 10 + rng.next_usize(40);
            let mut v = Vec::with_capacity(total_len);
            for _ in 0..total_len {
                v.push(rng.next_byte());
            }
            owned.push(v);
        }

        let keys: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();

        let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        expected.sort_unstable();
        let expected50 = expected.into_iter().take(50).collect::<Vec<_>>();

        let out = run_sort::<8>(&keys, Some(50));
        assert_eq!(out, expected50);
    }

    #[test]
    fn long_common_prefix_adversarial() {
        let mut owned = Vec::new();
        for i in 0..6000u32 {
            let mut v = vec![b'x'; 80];
            v.extend_from_slice(&i.to_be_bytes());
            owned.push(v);
        }
        let keys: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();

        let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        expected.sort_unstable();

        let out = run_sort::<8>(&keys, None);
        assert_eq!(out, expected);
    }

    #[test]
    fn prefix_ordering_rule() {
        let keys: Vec<&[u8]> = vec![b"a", b"aa", b"", b"ab"];
        let mut expected: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        expected.sort_unstable();

        let out = run_sort::<8>(&keys, None);
        assert_eq!(out, expected);
    }

    #[test]
    fn all_equal() {
        let owned: Vec<Vec<u8>> = (0..2000).map(|_| b"same".to_vec()).collect();
        let keys: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();

        let out = run_sort::<8>(&keys, None);
        assert_eq!(out.len(), 2000);
        assert!(out.iter().all(|k| k.as_slice() == b"same"));
    }
}
