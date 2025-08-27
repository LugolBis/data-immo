use chrono::NaiveDate;

pub fn parse_date(date_str: &str) -> Option<i32> {
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .ok()
        .map(|date| {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            date.signed_duration_since(epoch).num_days() as i32
        })
}
