// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::pending;

use futures::{Future, FutureExt, Stream};

/// Convert a list of streams into a [`Stream`] of results from the streams.
pub fn select_all<S: Stream + Unpin>(
    streams: impl IntoIterator<Item = S>,
) -> futures::stream::SelectAll<S> {
    // We simply forward the implementation to `futures` as it performs good enough.
    #[expect(clippy::disallowed_methods)]
    futures::stream::select_all(streams)
}

pub fn pending_on_none<I>(future: impl Future<Output = Option<I>>) -> impl Future<Output = I> {
    use futures::TryFutureExt;
    future
        .map(|opt| opt.ok_or(()))
        .or_else(|()| pending::<std::result::Result<I, ()>>())
        .map(|result| result.expect("only err on pending, which is unlikely to reach here"))
}
