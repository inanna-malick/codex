//! A remote transcript reader with no execution or server-request handling path.

use std::io;

use codex_app_server_client::AppServerEvent;
use codex_app_server_client::RemoteAppServerClient;
use codex_app_server_client::RemoteAppServerConnectArgs;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ControlState;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::SortDirection;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadItemsListParams;
use codex_app_server_protocol::ThreadItemsListResponse;
use codex_app_server_protocol::ThreadObserveParams;
use codex_app_server_protocol::ThreadObserveResponse;
use codex_app_server_protocol::ThreadTurnsListParams;
use codex_app_server_protocol::ThreadTurnsListResponse;
use codex_app_server_protocol::TurnItemsView;
use codex_utils_absolute_path::AbsolutePathBuf;
use crossterm::event::Event;
use crossterm::event::EventStream;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyEventKind;
use crossterm::event::KeyModifiers;
use crossterm::execute;
use crossterm::terminal::EnterAlternateScreen;
use crossterm::terminal::LeaveAlternateScreen;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Constraint;
use ratatui::layout::Layout;
use ratatui::style::Stylize;
use ratatui::text::Line;
use ratatui::widgets::Paragraph;
use tokio_stream::StreamExt;

use crate::thread_transcript::RawReasoningVisibility;
use crate::thread_transcript::thread_items_to_transcript_cells;

const MAX_ITEMS: usize = 1000;

struct Observer {
    thread_id: String,
    cwd: AbsolutePathBuf,
    items: Vec<ThreadItem>,
    older: Option<String>,
    older_items: Option<Vec<ThreadItem>>,
    status: String,
    scroll_back: usize,
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    None,
    Exit,
    History(Option<String>),
}

impl Observer {
    fn key(&mut self, key: KeyEvent) -> Action {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Action::Exit,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Action::Exit;
            }
            KeyCode::Up => self.scroll_back = self.scroll_back.saturating_add(3),
            KeyCode::Down => self.scroll_back = self.scroll_back.saturating_sub(3),
            KeyCode::End | KeyCode::PageDown => {
                self.scroll_back = 0;
                self.older_items = None;
            }
            KeyCode::PageUp if self.older_items.is_none() || self.older.is_some() => {
                // Returning from live view restarts history paging at the newest persisted page.
                return Action::History(self.older_items.as_ref().and(self.older.clone()));
            }
            _ => {}
        }
        Action::None
    }

    fn upsert(&mut self, item: ThreadItem) {
        if let Some(existing) = self
            .items
            .iter_mut()
            .find(|existing| existing.id() == item.id())
        {
            *existing = item;
        } else {
            self.items.push(item);
            if self.items.len() > MAX_ITEMS {
                self.items.remove(0);
            }
        }
    }

    fn notification(&mut self, notification: ServerNotification) {
        match notification {
            ServerNotification::ItemStarted(event) if event.thread_id == self.thread_id => {
                self.upsert(event.item)
            }
            ServerNotification::ItemCompleted(event) if event.thread_id == self.thread_id => {
                self.upsert(event.item)
            }
            ServerNotification::AgentMessageDelta(event) if event.thread_id == self.thread_id => {
                if let Some(ThreadItem::AgentMessage { text, .. }) = self
                    .items
                    .iter_mut()
                    .find(|item| item.id() == event.item_id)
                {
                    text.push_str(&event.delta);
                }
            }
            ServerNotification::CommandExecutionOutputDelta(event)
                if event.thread_id == self.thread_id =>
            {
                if let Some(ThreadItem::CommandExecution {
                    aggregated_output, ..
                }) = self
                    .items
                    .iter_mut()
                    .find(|item| item.id() == event.item_id)
                {
                    aggregated_output
                        .get_or_insert_default()
                        .push_str(&event.delta);
                }
            }
            ServerNotification::TurnCompleted(event) if event.thread_id == self.thread_id => {
                for item in event.turn.items {
                    self.upsert(item);
                }
            }
            ServerNotification::ControlStatusChanged(status)
                if status.0.state == ControlState::Fenced =>
            {
                self.status = "Execution fenced — controller disconnected; reconciliation required"
                    .to_string();
            }
            // Other notifications are informational and cannot cause outgoing actions.
            _ => {}
        }
    }

    fn lines(&self, width: u16) -> Vec<Line<'static>> {
        let cells = thread_items_to_transcript_cells(
            codex_protocol::ThreadId::from_string(&self.thread_id).ok(),
            &self.cwd,
            self.older_items.as_ref().unwrap_or(&self.items).clone(),
            RawReasoningVisibility::Hidden,
            /*config*/ None,
        );
        cells
            .into_iter()
            .flat_map(|cell| cell.display_lines(width))
            .collect()
    }

    fn render(&self, frame: &mut ratatui::Frame<'_>) {
        let [header, body, footer] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .areas(frame.area());
        frame.render_widget(Paragraph::new("Observer — read only".bold().cyan()), header);
        let lines = self.lines(body.width);
        let start = lines
            .len()
            .saturating_sub(body.height as usize)
            .saturating_sub(self.scroll_back);
        frame.render_widget(
            Paragraph::new(
                lines
                    .into_iter()
                    .skip(start)
                    .take(body.height as usize)
                    .collect::<Vec<_>>(),
            ),
            body,
        );
        frame.render_widget(
            Paragraph::new(
                format!(
                    "{}  ↑↓ scroll · PageUp history · End live · q exit",
                    self.status
                )
                .dim(),
            ),
            footer,
        );
    }
}

async fn load_page(
    client: &RemoteAppServerClient,
    thread_id: &str,
    mode: &ThreadHistoryMode,
    cursor: Option<String>,
    request_id: i64,
) -> color_eyre::Result<(Vec<ThreadItem>, Option<String>)> {
    if *mode == ThreadHistoryMode::Paginated {
        let page = client
            .request_typed::<ThreadItemsListResponse>(ClientRequest::ThreadItemsList {
                request_id: RequestId::Integer(request_id),
                params: ThreadItemsListParams {
                    thread_id: thread_id.to_string(),
                    turn_id: None,
                    cursor: cursor.clone(),
                    limit: Some(100),
                    sort_direction: Some(SortDirection::Desc),
                },
            })
            .await;
        match page {
            Ok(page) => {
                return Ok((
                    page.data
                        .into_iter()
                        .rev()
                        .map(|entry| entry.item)
                        .collect(),
                    page.next_cursor,
                ));
            }
            // New loaded threads can advertise paginated history before storage is materialized.
            Err(codex_app_server_client::TypedRequestError::Server { source, .. })
                if source.code == -32601 && cursor.is_none() => {}
            Err(error) => return Err(error.into()),
        }
    }
    let first_page = cursor.is_none();
    let page = client
        .request_typed::<ThreadTurnsListResponse>(ClientRequest::ThreadTurnsList {
            request_id: RequestId::Integer(request_id + 1),
            params: ThreadTurnsListParams {
                thread_id: thread_id.to_string(),
                cursor,
                limit: Some(5),
                sort_direction: Some(SortDirection::Desc),
                items_view: Some(TurnItemsView::Full),
            },
        })
        .await;
    match page {
        Ok(page) => Ok((
            page.data
                .into_iter()
                .rev()
                .flat_map(|turn| turn.items)
                .collect(),
            page.next_cursor,
        )),
        Err(codex_app_server_client::TypedRequestError::Server { source, .. })
            if first_page
                && source.code == -32600
                && source
                    .message
                    .starts_with(&format!("thread {thread_id} is not materialized yet;")) =>
        {
            Ok((Vec::new(), None))
        }
        Err(error) => Err(error.into()),
    }
}

struct RestoreTerminal;
impl Drop for RestoreTerminal {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// Observe an explicitly loaded execution without loading configuration or starting an executor.
pub async fn run_observer(thread_id: String, remote: String) -> color_eyre::Result<()> {
    let endpoint = crate::resolve_remote_addr(&remote)?;
    let mut client = RemoteAppServerClient::connect(RemoteAppServerConnectArgs {
        endpoint,
        client_name: "codex-observer".to_string(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        experimental_api: true,
        mcp_server_openai_form_elicitation: false,
        opt_out_notification_methods: Vec::new(),
        channel_capacity: crate::DEFAULT_IN_PROCESS_CHANNEL_CAPACITY,
    })
    .await?;
    let snapshot: ThreadObserveResponse = client
        .request_typed(ClientRequest::ThreadObserve {
            request_id: RequestId::Integer(1),
            params: ThreadObserveParams {
                thread_id: thread_id.clone(),
            },
        })
        .await?;
    let history_mode = snapshot.thread.history_mode;
    let mut observer = Observer {
        thread_id,
        cwd: snapshot.thread.cwd,
        items: snapshot
            .active_turn
            .into_iter()
            .flat_map(|turn| turn.items)
            .collect(),
        older: None,
        older_items: None,
        status: "Connected".to_string(),
        scroll_back: 0,
    };
    let mut request_id = 2;
    let (items, cursor) = load_page(
        &client,
        &observer.thread_id,
        &history_mode,
        None,
        request_id,
    )
    .await?;
    observer.older = cursor;
    let mut history = items
        .into_iter()
        .filter(|item| {
            !observer
                .items
                .iter()
                .any(|existing| existing.id() == item.id())
        })
        .collect::<Vec<_>>();
    history.append(&mut observer.items);
    let excess = history.len().saturating_sub(MAX_ITEMS);
    history.drain(..excess);
    observer.items = history;

    crossterm::terminal::enable_raw_mode()?;
    let _restore = RestoreTerminal;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut events = EventStream::new();
    loop {
        terminal.draw(|frame| observer.render(frame))?;
        tokio::select! {
            event = events.next() => match event {
                Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => match observer.key(key) {
                    Action::Exit => break,
                    Action::History(cursor) => {
                        request_id += 2;
                        let (mut items, cursor) = load_page(&client, &observer.thread_id, &history_mode, cursor, request_id).await?;
                        observer.older = cursor;
                        items.truncate(MAX_ITEMS);
                        observer.older_items = Some(items);
                        observer.scroll_back = 0;
                    }
                    Action::None => {},
                },
                Some(Ok(_)) => {},
                Some(Err(error)) => return Err(error.into()),
                None => break,
            },
            event = client.next_event() => match event {
                Some(AppServerEvent::ServerNotification(notification)) => observer.notification(*notification),
                Some(AppServerEvent::Lagged { .. }) => {
                    observer.status = "Output gap — exit and reattach to refresh history".to_string();
                }
                Some(AppServerEvent::ServerRequest(_)) => {
                    return Err(color_eyre::eyre::eyre!("service sent an execution request to an observer"));
                }
                Some(AppServerEvent::Disconnected { .. }) | None => {
                    return Err(color_eyre::eyre::eyre!("observer disconnected; execution was not resumed"));
                }
            }
        }
    }
    client.shutdown().await?;
    Ok(())
}

#[cfg(test)]
#[path = "observer_tests.rs"]
mod tests;
