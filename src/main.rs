// use ux::u4;
fn main() {
    // let index = 0 as usize;
    let hop = 1 as usize;
    println!("0b{:04b}", hop);
    for n in 0..32 {
        if (hop & (1 << n as usize)) != 0 {
            println!("{}", n);
            println!("T");
        } else {
            // println!("F");
        }
    }

    // hop |= 1 << (3 - 3 as u32);
    // println!("0b{:04b}", hop);
    // for i in 10..0 {
    //     println!("{}", i);

    // }
}