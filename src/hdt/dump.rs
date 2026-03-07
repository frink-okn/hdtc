#[cfg(test)]
mod tests {
    use crate::hdt::reader::{
        find_literal_boundary, write_escaped_literal_value, write_nt_object, write_nt_subject,
    };

    fn write_to_string(f: impl Fn(&mut Vec<u8>) -> std::io::Result<()>) -> String {
        let mut buf = Vec::new();
        f(&mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn test_write_subject_iri() {
        let result = write_to_string(|w| write_nt_subject(w, b"http://example.org/s"));
        assert_eq!(result, "<http://example.org/s>");
    }

    #[test]
    fn test_write_subject_blank_node() {
        let result = write_to_string(|w| write_nt_subject(w, b"_:b0"));
        assert_eq!(result, "_:b0");
    }

    #[test]
    fn test_write_object_iri() {
        let result = write_to_string(|w| write_nt_object(w, b"http://example.org/o"));
        assert_eq!(result, "<http://example.org/o>");
    }

    #[test]
    fn test_write_object_blank_node() {
        let result = write_to_string(|w| write_nt_object(w, b"_:b1"));
        assert_eq!(result, "_:b1");
    }

    #[test]
    fn test_write_literal_simple() {
        let result = write_to_string(|w| write_nt_object(w, b"\"hello\""));
        assert_eq!(result, "\"hello\"");
    }

    #[test]
    fn test_write_literal_typed() {
        let result = write_to_string(|w| {
            write_nt_object(w, b"\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        });
        assert_eq!(
            result,
            "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
    }

    #[test]
    fn test_write_literal_language() {
        let result = write_to_string(|w| write_nt_object(w, b"\"bonjour\"@fr"));
        assert_eq!(result, "\"bonjour\"@fr");
    }

    #[test]
    fn test_write_literal_embedded_quote() {
        let result = write_to_string(|w| write_nt_object(w, b"\"he said \"hi\"\""));
        assert_eq!(result, r#""he said \"hi\"""#);
    }

    #[test]
    fn test_write_literal_with_newline() {
        let result = write_to_string(|w| write_nt_object(w, b"\"line1\nline2\""));
        assert_eq!(result, "\"line1\\nline2\"");
    }

    #[test]
    fn test_write_literal_with_backslash() {
        let result = write_to_string(|w| write_nt_object(w, b"\"path\\to\\file\""));
        assert_eq!(result, "\"path\\\\to\\\\file\"");
    }

    #[test]
    fn test_write_literal_with_cr_and_tab() {
        let result = write_to_string(|w| write_nt_object(w, b"\"a\rb\tc\""));
        assert_eq!(result, "\"a\\rb\\tc\"");
    }

    #[test]
    fn test_write_literal_with_backspace_and_formfeed() {
        let result = write_to_string(|w| write_nt_object(w, b"\"a\x08b\x0Cc\""));
        assert_eq!(result, "\"a\\bb\\fc\"");
    }

    #[test]
    fn test_write_literal_with_other_control_chars() {
        let result = write_to_string(|w| write_nt_object(w, b"\"a\x00b\x07c\x0Bd\""));
        assert_eq!(result, "\"a\\u0000b\\u0007c\\u000Bd\"");
    }

    #[test]
    fn test_write_literal_typed_with_escapes() {
        let input = b"\"line1\nline2\"^^<http://www.w3.org/2001/XMLSchema#string>";
        let result = write_to_string(|w| write_nt_object(w, input));
        assert_eq!(
            result,
            "\"line1\\nline2\"^^<http://www.w3.org/2001/XMLSchema#string>"
        );
    }

    #[test]
    fn test_write_literal_language_with_escapes() {
        let input = b"\"line1\nline2\"@en";
        let result = write_to_string(|w| write_nt_object(w, input));
        assert_eq!(result, "\"line1\\nline2\"@en");
    }

    #[test]
    fn test_write_literal_unicode() {
        let result =
            write_to_string(|w| write_nt_object(w, "\"èpsilon\"".as_bytes()));
        assert_eq!(result, "\"èpsilon\"");
    }

    #[test]
    fn test_find_boundary_typed() {
        let term = b"\"value\"^^<http://example.org/type>";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(&term[ss..], b"^^<http://example.org/type>");
    }

    #[test]
    fn test_find_boundary_language() {
        let term = b"\"value\"@en";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(&term[ss..], b"@en");
    }

    #[test]
    fn test_find_boundary_simple() {
        let term = b"\"value\"";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"value");
        assert_eq!(ss, term.len());
    }

    #[test]
    fn test_find_boundary_value_containing_at() {
        let term = b"\"email@host\"@en";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"email@host");
        assert_eq!(&term[ss..], b"@en");
    }

    #[test]
    fn test_find_boundary_value_ending_with_at_no_lang_tag() {
        let term = b"\"user@\"";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"user@");
        assert_eq!(ss, term.len());
    }

    #[test]
    fn test_find_boundary_value_with_at_non_tag_suffix() {
        let term = b"\"user@host.com\"";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"user@host.com");
        assert_eq!(ss, term.len());
    }

    #[test]
    fn test_find_boundary_multiple_at_signs() {
        let term = b"\"a@fake\"@de";
        let (ve, ss) = find_literal_boundary(term);
        assert_eq!(&term[1..ve], b"a@fake");
        assert_eq!(&term[ss..], b"@de");
    }

    #[test]
    fn test_write_escaped_value_no_escape() {
        let result = write_to_string(|w| write_escaped_literal_value(w, b"hello world"));
        assert_eq!(result, "hello world");
    }
}
