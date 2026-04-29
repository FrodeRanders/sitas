use shardstar::ShardedKv;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kv = ShardedKv::start(4)?;

    let alpha = kv.submit_put("alpha", "one")?;
    let beta = kv.submit_put("beta", "two")?;

    alpha.wait()?;
    beta.wait()?;

    let alpha = kv.submit_get("alpha")?;
    let beta = kv.submit_get("beta")?;
    let total = kv.submit_total_len()?;

    println!("alpha = {:?}", alpha.wait()?);
    println!("beta = {:?}", beta.wait()?);
    println!("total keys = {}", total.wait()?);

    kv.stop()?;

    Ok(())
}
