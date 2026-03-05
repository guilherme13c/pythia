use serde::Deserialize;

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub lang: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_params_parsing() {
        let query_string = "q=machine+learning&limit=50&offset=10&lang=fr";
        let params: SearchParams = serde_urlencoded::from_str(query_string).unwrap();

        assert_eq!(params.q, "machine learning");
        assert_eq!(params.limit, Some(50));
        assert_eq!(params.offset, Some(10));
        assert_eq!(params.lang.as_deref(), Some("fr"));

        let query_string_no_limit = "q=rust+lang";
        let params_no_limit: SearchParams =
            serde_urlencoded::from_str(query_string_no_limit).unwrap();

        assert_eq!(params_no_limit.q, "rust lang");
        assert_eq!(params_no_limit.limit, None);
        assert_eq!(params_no_limit.offset, None);
    }
}
