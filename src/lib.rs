use pathfinding::prelude::{Matrix, kuhn_munkres_min};
use std::{
    cmp,
    process::Command,
};

// https://stackoverflow.com/questions/34837011/how-to-clear-the-terminal-screen-in-rust-after-a-new-line-is-printed
// https://stackoverflow.com/questions/65497187/cant-run-a-system-command-in-windows
pub fn clear_terminal_screen() {
    if cfg!(target_os = "windows") {
        Command::new("cmd")
            .args(["/c", "cls"])
            .spawn()
            .expect("cls command failed to start")
            .wait()
            .expect("failed to wait");
    } else {
        Command::new("clear")
            .spawn()
            .expect("clear command failed to start")
            .wait()
            .expect("failed to wait");
    };
}

pub trait ExtraProperties {
    fn chars_count(self) -> usize;
    fn contains_only_digit(self) -> bool;
    fn contains_some_digit(self) -> bool;
    fn contains_num_digit(self, num_digit: usize) -> bool;
    fn replace_multiple_whitespaces(self) -> String;
    fn select_first_digits(self) -> String;
}

impl ExtraProperties for &str {
    fn chars_count(self) -> usize {
        // Not use self.len()
        self.chars().count()
    }

    fn contains_only_digit(self) -> bool {
        !self.is_empty() &&
        self.bytes().all(|x| x.is_ascii_digit())
    }

    fn contains_some_digit(self) -> bool {
        self.bytes().any(|x| x.is_ascii_digit())
    }

    fn contains_num_digit(self, num_digit: usize) -> bool {
        self.chars_count() == num_digit &&
        self.bytes().all(|x| x.is_ascii_digit())
    }

    // https://stackoverflow.com/questions/71864137/whats-the-ideal-way-to-trim-extra-spaces-from-a-string
    // Replace multiple whitespace '   ' with single whitespace ' '
    // Substituir dois ou mais espaços em branco por apenas um
    fn replace_multiple_whitespaces(self) -> String {
        let mut new_str: String = self.to_string();
        let mut previous_char: char = 'x'; // some non-whitespace character
        new_str.retain(|current_char| {
            //let keep: bool = !(previous_char == ' ' && current_char == ' ');
            let keep: bool = previous_char != ' ' || current_char != ' ';
            previous_char = current_char;
            keep
        });
        new_str
    }

    // Capturar ou Reter apenas o primeiro grupo de dígitos: 1191-1  --> 1191  ou 10845/a --> 10845
    fn select_first_digits(self) -> String {
        self
        .chars()
        .map_while(|x| x.is_ascii_digit().then_some(x))
        .collect::<String>()
    }
}


/* --- munkres --- */
/* ---- start ---- */

pub fn get_width<T>(array1: &[T], array2: &[T]) -> usize
    where T: Copy + Clone + ToString + PartialOrd
{
    let concatenated: Vec<T> = [array1, array2].concat();
    let mut max_value: T = concatenated[0];
    for value in concatenated {
        if value > max_value {
            max_value = value;
        }
    }
    let width: usize = max_value.to_string().len();
    width
}

pub fn get_matrix(array1: &[i128], array2: &[i128]) -> Vec<Vec<i128>> {
    let mut matrix = Vec::new();
    for a1 in array1 {
        let mut array = Vec::new();
        for a2 in array2 {
            let delta: i128 = a1 - a2;
            array.push(delta.pow(2));  // valor ao quadrado
            //array.push(delta.abs()); // valor absoluto
        }
        matrix.push(array);
    }
    matrix
}

// https://stackoverflow.com/questions/59314686/how-to-efficiently-create-a-large-vector-of-items-initialized-to-the-same-value
// https://stackoverflow.com/questions/29530011/creating-a-vector-of-zeros-for-a-specific-size

pub fn convert_to_square_matrix(matrix: &mut Vec<Vec<i128>>) {

    // check if the matrix is a square matrix,
    // if not convert it to square matrix by padding zeroes.

    let row_number: usize = matrix.len();
    let col_number: usize = matrix[0].len();
    let delta: usize = row_number.abs_diff(col_number);
    let _min: usize = cmp::min(row_number, col_number);

    //println!("row_number: {row_number}");
    //println!("col_number: {col_number}");
    //println!("delta: {delta}\n");

    if row_number < col_number { // Add rows
        for _ in 0 .. delta {
            let vector = vec![0; col_number];
            matrix.push(vector);
        }
    }

    if row_number > col_number { // Add columns
        for vector in &mut matrix[..] {
            let zeroes = vec![0; delta];
            vector.extend(zeroes);
        }
    }
}

pub fn display_bipartite_matching (
    width: usize,
    matrix: &[Vec<i128>],
    array1: &[i128],
    array2: &[i128],
    assignments: &[usize],
    filter: bool,
) -> i128 {

    let row_number: usize = array1.len();
    let col_number: usize = array2.len();
    let min: usize = cmp::min(row_number, col_number);
    let max: usize = cmp::max(row_number, col_number);
    let widx = max.to_string().len();

    let mut bipartite: Vec<(i128, i128, u128)> = Vec::new();
    let mut assign: Vec<usize> = Vec::new(); // assignments after filter
    let mut values: Vec<i128> = Vec::new();
    let mut sum = 0;

    // https://doc.rust-lang.org/std/vec/struct.Vec.html#method.retain
    // assignments.to_vec().retain(|&col| col < min);

    for (row, &col) in assignments.iter().enumerate() {

        if filter && ((row_number > col_number && col >= min) || (row_number < col_number && row >= min)) {
            continue;
        }

        let value = matrix[row][col];
        values.push(value);
        assign.push(col);
        sum += value;
    }

    let width_index: usize = get_width(&assign, &[]);
    let width_value: usize = get_width(&[], &values);
    let width_b: usize = width_index.max(width_value);

    println!("matrix indexes: {assign:>width_b$?}");
    println!("matrix values:  {values:>width_b$?}");
    println!("sum of values: {sum}\n");

    for (row, &col) in assignments.iter().enumerate() {

        if (row_number > col_number && col >= min) || (row_number < col_number && row >= min) {
            continue;
        }

        let delta: u128 = array1[row].abs_diff(array2[col]);
        println!("(array1[{row:widx$}], array2[{col:widx$}], abs_diff): ({:>width$}, {:>width$}, {delta:>width$})", array1[row], array2[col]);
        bipartite.push((array1[row], array2[col], delta));
    }
    println!();

    sum
}

pub fn print_matrix(
    width: usize,
    matrix: &[Vec<i128>],
    array1: &[i128],
    array2: &[i128],
    assignments: &[usize],
    filter: bool,
) {

    let row_number: usize = array1.len();
    let col_number: usize = array2.len();
    let min: usize = cmp::min(row_number, col_number);

    println!("Matriz do módulo da diferença, matriz[i][j] = abs (array1[i] - array2[j]):\n");

    print!("{:>w$}", ' ', w = width + 2);
    for val in array2 {
        print!(" {val:>width$}, ");
    }
    println!("\n");

    for (i, vec) in matrix.iter().enumerate() {
        let mut val = format!("{:>width$}", ' ');
        if i < array1.len() {
            val = format!("{:>width$}", array1[i]);
        }
        print!("{val} [");

        let mut vector: Vec<i128> = vec.to_vec();
        if filter && (row_number > col_number) {
            vector.truncate(min);
        }

        let idx = assignments[i];
        for (j, val) in vector.iter().enumerate() {
            if j == idx {
                let star: String = vec!["*".to_string(); 1 + width - val.to_string().len()].join("");
                let new_val = [star, val.to_string()].concat(); // add *
                print!("{new_val:>width$}"); // add *
            } else {
                print!(" {val:>width$}");
            }
            if j < (vector.len() - 1) {
                print!(", ");
            }
        }
        println!(" ]");

        if filter && (row_number < col_number && i >= (min - 1)) {
            // println!("i: {i} ; min: {min}\n");
            break;
        }
    }

    println!();
}

/* ---- final ---- */
/* --- munkres --- */

/// Get Series of minimal Munkres Assignments from two f64 Slices
pub fn munkres_assignments(tuple_a: &[(usize, f64)], tuple_b: &[(usize, f64)]) -> Vec<usize> {

    let array_1: Vec<i128> = tuple_a.iter().map(|&v| (v.1 * 100.0).round() as i128).collect();
    let array_2: Vec<i128> = tuple_b.iter().map(|&v| (v.1 * 100.0).round() as i128).collect();

    //let width: usize = get_width(&array_1, &array_2);
    //println!("\nFind the minimum bipartite matching:");
    //println!("array_1: {array_1:width$?}");
    //println!("array_2: {array_2:width$?}");

    let mut matrix: Vec<Vec<i128>> = get_matrix(&array_1, &array_2);

    convert_to_square_matrix(&mut matrix);

    // Assign weights to everybody choices
    let weights: Matrix<i128> = Matrix::from_rows(matrix.clone()).unwrap();
    let (_sum, assignments): (i128, Vec<usize>) = kuhn_munkres_min(&weights);

    //display_bipartite_matching(width, &matrix, &array_1, &array_2, &assignments, false);
    //print_matrix(width, &matrix[..], &array_1, &array_2, &assignments, true);

    assignments
}