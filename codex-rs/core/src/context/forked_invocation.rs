use super::ContextualUserFragment;
use codex_protocol::models::ContentItemKind;

pub(crate) struct ForkedInvocation;

impl ContextualUserFragment for ForkedInvocation {
    fn content_kind(&self) -> ContentItemKind {
        ContentItemKind("fork.invocation_boundary".to_string())
    }

    fn role(&self) -> &'static str {
        "user"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        ("", "")
    }

    fn body(&self) -> String {
        "Context fork boundary. This invocation belongs to the source conversation; its result is outside the inherited snapshot. This is a child-local protocol closure, not a tool result or a failure of the source operation. Do not replay the invocation or infer returned values. Await this conversation's own assignment.".to_string()
    }
}
