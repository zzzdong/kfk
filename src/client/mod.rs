mod admin;
mod factory;
mod types;

pub use admin::*;
pub use factory::*;
pub use types::*;

/// Common result type for CLI operations
pub type CliResult<T> = Result<T, String>;
