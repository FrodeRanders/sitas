use sitas::executor::block_on;
use sitas::{ShardError, ShardedKv};

fn main() -> Result<(), ShardError> {
    block_on(async {
        let kv = ShardedKv::start(4)?;

        kv.submit_put("alpha", "one")?.wait_async().await?;
        kv.submit_put("beta", "two")?.wait_async().await?;

        let alpha = kv.submit_get("alpha")?.wait_async().await?;
        println!("alpha = {:?}", alpha);

        let values = kv
            .submit_get_many(["beta", "alpha", "missing"])?
            .wait_async()
            .await?;
        println!("selected values = {:?}", values);

        let total = kv.submit_total_len()?.wait_async().await?;
        println!("total keys = {total}");

        kv.stop()
    })
}
