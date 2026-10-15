use streamforge::Partition;
fn main() -> std::io::Result<()> {
    let mut p = Partition::open("streamforge.log")?;
    println!("offset={}", p.append(b"hello")?);
    Ok(())
}
