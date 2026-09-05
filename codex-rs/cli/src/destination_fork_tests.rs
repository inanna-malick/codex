use super::*;
use pretty_assertions::assert_eq;

#[test]
fn destination_fork_is_explicit_with_or_without_effort() {
    let temporary = tempfile::tempdir().unwrap();
    let socket = temporary.path().join("host.sock");
    for effort in [None, Some("low"), Some("high"), Some("xhigh")] {
        let mut args = vec![
            "codex".to_string(),
            "fork".to_string(),
            "source-uuid".to_string(),
            "--destination-local".to_string(),
            "--through-call".to_string(),
            "call_1".to_string(),
            "--host-dynamic-tools-socket".to_string(),
            socket.to_string_lossy().into_owned(),
        ];
        if let Some(effort) = effort {
            args.extend([
                "-c".to_string(),
                format!("model_reasoning_effort=\"{effort}\""),
            ]);
        }
        let cli = MultitoolCli::try_parse_from(args).unwrap();
        let Some(Subcommand::Fork(fork)) = cli.subcommand else {
            panic!("expected fork")
        };
        assert_eq!(
            (fork.destination_local, fork.through_call.as_deref()),
            (true, Some("call_1"))
        );
    }
}

#[test]
fn destination_fork_requires_an_exact_boundary_and_host() {
    for args in [
        vec!["codex", "fork", "source-uuid", "--destination-local"],
        vec![
            "codex",
            "fork",
            "source-uuid",
            "--destination-local",
            "--through-call",
            "call_1",
        ],
    ] {
        assert_eq!(
            MultitoolCli::try_parse_from(args).unwrap_err().kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }
}

#[test]
fn destination_fork_help_documents_the_boundary_and_isolation() {
    let help = MultitoolCli::command()
        .term_width(80)
        .try_get_matches_from(["codex", "fork", "--help"])
        .unwrap_err();
    assert_eq!(help.kind(), clap::error::ErrorKind::DisplayHelp);
    insta::assert_snapshot!(
        help.to_string()
            .lines()
            .map(str::trim_end)
            .collect::<Vec<_>>()
            .join("\n")
    );
}
