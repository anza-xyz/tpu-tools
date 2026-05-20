//! Shared building blocks for Solana TPU benchmarking tools.
//!
//! This crate contains the account setup, blockhash refresh, command-line
//! parsing, and leader tracking code used by `solana-transaction-bench` and
//! `solana-rate-latency-tool`.
//!
//! Most users should run those binaries directly. Use this crate when building
//! related TPU tooling that needs the same runtime behavior.
//!
//! # Main Modules
//!
//! - [`accounts_file`]: read and write payer account files, and create
//!   ephemeral or file-persisted payer accounts.
//! - [`accounts_creator`]: RPC-backed payer account creation.
//! - [`cli`]: shared Clap argument structs and parsers.
//! - [`blockhash_updater`]: background RPC polling for fresh blockhashes.
//! - [`leader_updater`]: leader tracking traits and factory function.
//! - [`yellowstone_leader_tracker`]: Yellowstone gRPC slot-event adapter for
//!   `solana-tpu-client-next`.
//!
//! # Stability
//!
//! APIs follow the TPU tools release cadence and may change as Solana/Agave
//! release candidate dependencies evolve.

pub mod accounts_creator;
pub mod accounts_file;
pub mod blockhash_updater;
pub mod cli;
mod custom_geyser_node_address_service;
pub mod leader_updater;
pub mod yellowstone_leader_tracker;
