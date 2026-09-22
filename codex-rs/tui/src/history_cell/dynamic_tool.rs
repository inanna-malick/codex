//! Dynamic tool-call history cells.

use super::*;

use codex_app_server_protocol::DynamicToolCallOutputContentItem;

const DISPLAY_INPUT_LINES: usize = 8;
const DISPLAY_OUTPUT_LINES: usize = 5;

#[derive(Debug)]
pub(crate) struct DynamicToolCallCell {
    call_id: String,
    namespace: Option<String>,
    tool: String,
    arguments: serde_json::Value,
    start_time: Instant,
    duration: Option<Duration>,
    result: Option<DynamicToolResult>,
    animations_enabled: bool,
}

#[derive(Debug)]
struct DynamicToolResult {
    content_items: Vec<DynamicToolCallOutputContentItem>,
    success: bool,
}

impl DynamicToolCallCell {
    pub(crate) fn new(
        call_id: String,
        namespace: Option<String>,
        tool: String,
        arguments: serde_json::Value,
        animations_enabled: bool,
    ) -> Self {
        Self {
            call_id,
            namespace,
            tool,
            arguments,
            start_time: Instant::now(),
            duration: None,
            result: None,
            animations_enabled,
        }
    }

    pub(crate) fn call_id(&self) -> &str {
        &self.call_id
    }

    pub(crate) fn complete(
        &mut self,
        duration: Duration,
        content_items: Vec<DynamicToolCallOutputContentItem>,
        success: bool,
    ) {
        self.duration = Some(duration);
        self.result = Some(DynamicToolResult {
            content_items,
            success,
        });
    }

    pub(crate) fn mark_failed(&mut self) {
        self.duration = Some(self.start_time.elapsed());
        self.result = Some(DynamicToolResult {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: "interrupted".to_string(),
            }],
            success: false,
        });
    }

    fn qualified_name(&self) -> String {
        self.namespace
            .as_ref()
            .map(|namespace| format!("{namespace}.{}", self.tool))
            .unwrap_or_else(|| self.tool.clone())
    }

    fn input(&self) -> (String, &'static str) {
        match &self.arguments {
            serde_json::Value::String(input) => (input.clone(), ""),
            arguments => (
                serde_json::to_string_pretty(arguments).unwrap_or_else(|_| arguments.to_string()),
                "json",
            ),
        }
    }

    fn output(&self) -> Option<String> {
        let result = self.result.as_ref()?;
        let output = result
            .content_items
            .iter()
            .map(|item| match item {
                DynamicToolCallOutputContentItem::InputText { text } => text.clone(),
                DynamicToolCallOutputContentItem::InputImage { .. } => "<image output>".to_string(),
                DynamicToolCallOutputContentItem::InputAudio { .. } => "<audio output>".to_string(),
            })
            .collect::<Vec<_>>()
            .join("\n");
        Some(output)
    }

    fn render_lines(&self, full: bool) -> Vec<Line<'static>> {
        let (bullet, state) = match self.result.as_ref() {
            Some(result) if result.success => ("•".green().bold(), "Completed"),
            Some(_) => ("•".red().bold(), "Failed"),
            None => (
                activity_indicator(
                    Some(self.start_time),
                    MotionMode::from_animations_enabled(self.animations_enabled),
                    ReducedMotionIndicator::StaticBullet,
                )
                .unwrap_or_else(|| "•".dim()),
                "Running",
            ),
        };
        let mut header = vec![
            bullet,
            " ".into(),
            self.qualified_name().cyan(),
            " ".into(),
            state.bold(),
        ];
        if let Some(duration) = self.duration {
            let duration = if duration.as_secs() == 0 {
                format!("{}ms", duration.as_millis())
            } else {
                format!("{:.1}s", duration.as_secs_f64())
            };
            header.push(format!(" · {duration}").dim());
        }

        let mut lines = vec![Line::from(header)];
        let (input, fence_language) = self.input();
        lines.push(vec!["  ├ ".dim(), "Input".bold()].into());
        lines.push(format!("  │ ```{fence_language}").dim().into());
        lines.extend(render_block(&input, "  │ ", DISPLAY_INPUT_LINES, full));
        lines.push("  │ ```".dim().into());

        if let Some(output) = self.output() {
            lines.push(vec!["  └ ".dim(), "Result".bold()].into());
            if output.is_empty() {
                lines.push("    <no output>".dim().into());
            } else {
                lines.extend(render_block(&output, "    ", DISPLAY_OUTPUT_LINES, full));
            }
        }
        lines
    }
}

fn render_block(
    text: &str,
    prefix: &'static str,
    max_lines: usize,
    full: bool,
) -> Vec<Line<'static>> {
    let source_lines = raw_lines_from_source(text);
    let truncated = !full && source_lines.len() > max_lines;
    let visible_lines = if truncated {
        &source_lines[..max_lines]
    } else {
        source_lines.as_slice()
    };
    let mut lines = visible_lines
        .iter()
        .map(|line| {
            let mut spans = vec![prefix.dim()];
            spans.extend(line.spans.clone());
            Line::from(spans)
        })
        .collect::<Vec<_>>();
    if text.is_empty() {
        lines.push(prefix.dim().into());
    } else if truncated {
        lines.push(
            vec![
                prefix.dim(),
                format!(
                    "… {} lines hidden (Ctrl+T for full transcript)",
                    source_lines.len() - max_lines
                )
                .dim(),
            ]
            .into(),
        );
    }
    lines
}

impl HistoryCell for DynamicToolCallCell {
    fn display_lines(&self, _width: u16) -> Vec<Line<'static>> {
        self.render_lines(/*full*/ false)
    }

    fn transcript_lines(&self, _width: u16) -> Vec<Line<'static>> {
        self.render_lines(/*full*/ true)
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        plain_lines(self.render_lines(/*full*/ true))
    }

    fn transcript_animation_tick(&self) -> Option<u64> {
        if !self.animations_enabled || self.result.is_some() {
            return None;
        }
        Some((self.start_time.elapsed().as_millis() / 50) as u64)
    }
}

#[cfg(test)]
#[path = "dynamic_tool_tests.rs"]
mod tests;
