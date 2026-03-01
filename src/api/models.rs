use serde::Deserialize;

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    pub limit: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_params_parsing() {
        let query_string = "q=machine+learning&limit=50";
        let params: SearchParams = serde_urlencoded::from_str(query_string).unwrap();

        assert_eq!(params.q, "machine learning");
        assert_eq!(params.limit, Some(50));

        let query_string_no_limit = "q=rust+lang";
        let params_no_limit: SearchParams =
            serde_urlencoded::from_str(query_string_no_limit).unwrap();

        assert_eq!(params_no_limit.q, "rust lang");
        assert_eq!(params_no_limit.limit, None);
    }
}
