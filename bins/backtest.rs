#[path = "../src/data_source.rs"]
mod data_source;
#[path = "../src/parquet_loader.rs"]
mod parquet_loader;

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use data_source::DataSource;
use parquet_loader::{list_parquet_files, load_parquet_file_events, Level};

const TRIGGER_PROB: f64 = 0.97;
const TIME_TRIGGER_SECS: f64 = 60.0;
const ORDER_SIZE_USD: f64 = 20.0;
const LATENCY_SECS: f64 = 0.0008;

#[derive(Debug, Clone, Default)]
struct BookState {
    bids: Vec<Level>,
    asks: Vec<Level>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    winning_outcome: Option<String>,
    close_ts: f64,
}

#[derive(Debug, Clone)]
struct PendingTrigger {
    market_slug: String,
    arrival_ts: f64,
    close_ts: f64,
    theoretical_price: f64,
    contracts: f64,
    win_if_yes: bool,
}

#[derive(Debug, Clone)]
struct SimulatedFill {
    avg_fill_price: f64,
    fill_contracts: f64,
    ask_liquidity: f64,
    bid_depth_top10: f64,
    best_bid: Option<f64>,
}

fn main() -> anyhow::Result<()> {
    let parquet_dir = std::env::var("L2_PARQUET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data/l2"));

    let data_source = DataSource::Backtest {
        parquet_dir: parquet_dir.clone(),
    };
    let files = match &data_source {
        DataSource::Backtest { parquet_dir } => list_parquet_files(parquet_dir)?,
        DataSource::Live { .. } => unreachable!(),
    };

    let mut total_triggers = 0_u64;
    let mut simulated_fills = 0_u64;
    let mut wins_on_fills = 0_u64;
    let mut theoretical_pnl = 0.0_f64;
    let mut honest_pnl = 0.0_f64;
    let mut debug_printed = 0_u64;
    let mut files_processed = 0_u64;

    for path in files {
        let events = load_parquet_file_events(&path)?;
        if events.is_empty() {
            continue;
        }

        files_processed += 1;
        let market_slug = events[0].market_slug.clone();
        let mut book_state = BookState {
            close_ts: events[0].market_close_ts,
            ..Default::default()
        };
        let mut pending: Option<PendingTrigger> = None;
        let mut triggered = false;

        for event in events {
            if !event.bid_levels.is_empty() {
                book_state.bids = sort_desc(event.bid_levels.clone());
            }
            if !event.ask_levels.is_empty() {
                book_state.asks = sort_asc(event.ask_levels.clone());
            }
            book_state.best_bid = event
                .best_bid
                .or_else(|| book_state.bids.first().map(|level| level.price))
                .or(book_state.best_bid);
            book_state.best_ask = event
                .best_ask
                .or_else(|| book_state.asks.first().map(|level| level.price))
                .or(book_state.best_ask);
            if let Some(outcome) = event.winning_outcome.clone() {
                book_state.winning_outcome = Some(outcome);
            }
            book_state.close_ts = event.market_close_ts;

            if let Some(trigger) = pending.clone() {
                if event.timestamp >= trigger.arrival_ts {
                    settle_trigger(
                        &book_state,
                        &trigger,
                        &mut simulated_fills,
                        &mut wins_on_fills,
                        &mut honest_pnl,
                        &mut debug_printed,
                    );
                    pending = None;
                }
            }

            if triggered {
                continue;
            }

            let Some(observed_price) = event.observed_price() else {
                continue;
            };

            let secs_remaining = event.market_close_ts - event.timestamp;
            if observed_price < TRIGGER_PROB || !(0.0..TIME_TRIGGER_SECS).contains(&secs_remaining)
            {
                continue;
            }

            triggered = true;
            total_triggers += 1;

            let theoretical_price = book_state
                .best_ask
                .or(event.best_ask)
                .unwrap_or(observed_price)
                .clamp(0.001, 0.999);
            let contracts = ORDER_SIZE_USD / theoretical_price;
            let win_if_yes = true;
            let theoretical_win = book_state
                .winning_outcome
                .as_deref()
                .map(is_positive_outcome)
                .unwrap_or(false)
                == win_if_yes;
            theoretical_pnl += pnl_for_fill(theoretical_win, contracts, theoretical_price);

            pending = Some(PendingTrigger {
                market_slug: market_slug.clone(),
                arrival_ts: event.timestamp + LATENCY_SECS,
                close_ts: event.market_close_ts,
                theoretical_price,
                contracts,
                win_if_yes,
            });
        }

        if let Some(trigger) = pending {
            settle_trigger(
                &book_state,
                &trigger,
                &mut simulated_fills,
                &mut wins_on_fills,
                &mut honest_pnl,
                &mut debug_printed,
            );
        }
    }

    let fill_rate = simulated_fills as f64 / total_triggers.max(1) as f64;
    let win_rate = wins_on_fills as f64 / simulated_fills.max(1) as f64;
    let avg_profit_per_fill = honest_pnl / simulated_fills.max(1) as f64;

    println!("\n{}", "=".repeat(60));
    println!("BACKTEST RESULTS");
    println!("{}", "=".repeat(60));
    println!("Files processed:          {}", files_processed);
    println!("Total triggers:           {}", total_triggers);
    println!(
        "Fills:                    {} ({:.2}% fill rate)",
        simulated_fills,
        fill_rate * 100.0
    );
    println!("Win rate (on fills):      {:.2}%", win_rate * 100.0);
    println!("Theoretical PnL:          ${:.4}", theoretical_pnl);
    println!("Honest PnL:               ${:.4}", honest_pnl);
    println!("Average profit / fill:    ${:.4}", avg_profit_per_fill);

    Ok(())
}

fn sort_asc(mut levels: Vec<Level>) -> Vec<Level> {
    levels.sort_by(|left, right| left.price.partial_cmp(&right.price).unwrap());
    levels
}

fn sort_desc(mut levels: Vec<Level>) -> Vec<Level> {
    levels.sort_by(|left, right| right.price.partial_cmp(&left.price).unwrap());
    levels
}

fn simulate_fill(state: &BookState, contracts_needed: f64) -> Option<SimulatedFill> {
    if contracts_needed <= 0.0 || state.asks.is_empty() {
        return None;
    }

    let mut remaining = contracts_needed;
    let mut notional = 0.0;
    let mut filled = 0.0;
    for level in &state.asks {
        if remaining <= 0.0 {
            break;
        }
        let take = remaining.min(level.size);
        notional += take * level.price;
        filled += take;
        remaining -= take;
    }

    if filled + 1e-9 < contracts_needed {
        return None;
    }

    Some(SimulatedFill {
        avg_fill_price: notional / filled,
        fill_contracts: filled,
        ask_liquidity: total_liquidity(&state.asks),
        bid_depth_top10: top10_depth(&state.bids),
        best_bid: state.best_bid,
    })
}

fn settle_trigger(
    book_state: &BookState,
    trigger: &PendingTrigger,
    simulated_fills: &mut u64,
    wins_on_fills: &mut u64,
    honest_pnl: &mut f64,
    debug_printed: &mut u64,
) {
    let fill = simulate_fill(book_state, trigger.contracts);
    if let Some(fill) = fill {
        *simulated_fills += 1;
        let win = book_state
            .winning_outcome
            .as_deref()
            .map(is_positive_outcome)
            .unwrap_or(false)
            == trigger.win_if_yes;
        if win {
            *wins_on_fills += 1;
        }
        *honest_pnl += pnl_for_fill(win, fill.fill_contracts, fill.avg_fill_price);
    } else if *debug_printed < 5 {
        *debug_printed += 1;
        println!(
            "DEBUG fill miss #{}: market={} best_bid={:?} bid_depth_top10={:.4} sell_volume={:.4}",
            *debug_printed,
            trigger.market_slug,
            book_state.best_bid,
            top10_depth(&book_state.bids),
            total_liquidity(&book_state.asks)
        );
    }
}

fn pnl_for_fill(win: bool, contracts: f64, fill_price: f64) -> f64 {
    if win {
        contracts * (1.0 - fill_price)
    } else {
        -contracts * fill_price
    }
}

fn is_positive_outcome(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "yes" | "true" | "up" | "winner" | "won"
    )
}

fn total_liquidity(levels: &[Level]) -> f64 {
    levels.iter().map(|level| level.size).sum()
}

fn top10_depth(levels: &[Level]) -> f64 {
    levels.iter().take(10).map(|level| level.size).sum()
}
