package raft

import raft.Message.*
import java.lang.Integer.min
import java.util.*

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Evgeniy Kosogorov
 */
class ProcessImpl(private val env: Environment) : Process {
    private enum class State {
        FOLLOWER, CANDIDATE, LEADER
    }

    private var commands: Queue<Command> = ArrayDeque()
    private var leader: Int? = null
    private var state: State = State.FOLLOWER
    private val storage = env.storage
    private val machine = env.machine
    private var votes = 0
    private var commitIndex = 0
    private var lastApplied = 0
    private var matchIndex = IntArray(1) { 0 }
    private var nextIndex = IntArray(1) { env.storage.readLastLogId().index }

    private fun becomeFollower() {
//        if (state == State.FOLLOWER) return
        votes = 0
        state = State.FOLLOWER
        leader = null
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    init {
        becomeFollower()
    }

    private fun startElection() {
        votes = 1
        leader = null
        val persistentState = storage.readPersistentState()
        val newState = persistentState.copy(
            currentTerm = persistentState.currentTerm + 1,
            votedFor = env.processId
        )
        storage.writePersistentState(newState)
        for (pid in 1..env.nProcesses) {
            if (pid != env.processId) {
                env.send(pid, RequestVoteRpc(newState.currentTerm, storage.readLastLogId()))
            }
        }
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    private fun heartBeat(persistentState: PersistentState) {
        for (pid in 1..env.nProcesses) {
            if (pid != env.processId) {
                env.send(
                    pid, AppendEntryRpc(
                        persistentState.currentTerm,
                        storage.readLastLogId(),
                        commitIndex,
                        null
                    )
                )
            }
        }
    }

    override fun onTimeout() {
        initial()
        when (state) {
            State.FOLLOWER -> {
                state = State.CANDIDATE
                startElection()
            }
            State.CANDIDATE -> {
                startElection()
            }
            State.LEADER -> {
                initial()
                heartBeat(storage.readPersistentState())
                env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
            }
        }
    }

    private fun initial() {
        if (state == State.LEADER) {
            while (commands.isNotEmpty()) {
                val command = commands.poll()
                onCommand(command)
            }
        }
        val term = storage.readPersistentState().currentTerm
        while (commitIndex > lastApplied) {
            val log = storage.readLog(lastApplied + 1)!!
            lastApplied++
            val command = log.command
            val result = machine.apply(command)
            if (state == State.LEADER && log.id.term == term) {
                if (command.processId == env.processId) {
                    env.onClientCommandResult(result)
                } else {
                    env.send(command.processId, ClientCommandResult(term, result))
                }
            }
        }
        if (leader != null) {
            while (commands.isNotEmpty()) {
                val command = commands.poll()
                env.send(leader!!, ClientCommandRpc(term, command))
            }
        }
        if (state == State.LEADER && leader != null) {
            throw Throwable("WAT?")
        }
    }

    private fun initLeader() {
        votes = 0
        leader = null
        matchIndex = IntArray(env.nProcesses + 1) { 0 }
        nextIndex = IntArray(env.nProcesses + 1) { storage.readLastLogId().index + 1 }
    }

    private fun appendEntry(srcId: Int) {
        val lastLogId = storage.readLastLogId()
        val persistentState = storage.readPersistentState()
        if (nextIndex[srcId] <= lastLogId.index) {
            val prevLog = storage.readLog(nextIndex[srcId] - 1)
            val prevLogId = prevLog?.id ?: START_LOG_ID
            env.send(
                srcId,
                AppendEntryRpc(
                    persistentState.currentTerm,
                    prevLogId,
                    commitIndex,
                    storage.readLog(nextIndex[srcId])
                )
            )
        }
    }

    private fun onCommand(command: Command) {
        val term = storage.readPersistentState().currentTerm
        when {
            state == State.LEADER -> {
                val lastLogId = storage.readLastLogId()
                storage.appendLogEntry(
                    LogEntry(
                        LogId(lastLogId.index + 1, term),
                        command
                    )
                )
                for (pid in 1..env.nProcesses) {
                    if (pid != env.processId && nextIndex[pid] == lastLogId.index + 1) {
                        appendEntry(pid)
                    }
                }
            }
            leader != null -> {
                env.send(leader!!, ClientCommandRpc(term, command))
            }
            else -> {
                commands.add(command)
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        initial()
        val pst = storage.readPersistentState()
        val newTerm: Boolean
        if (pst.currentTerm < message.term) {
            storage.writePersistentState(
                pst.copy(
                    currentTerm = message.term,
                    votedFor = null))
            becomeFollower()
            newTerm = true
        } else {
            newTerm = false
        }
        val persistentState = storage.readPersistentState()
        when (message) {
            is AppendEntryResult -> if (state == State.LEADER) {
                if (message.lastIndex == null) {
                    nextIndex[srcId]--
                    if (matchIndex[srcId] >= nextIndex[srcId]) {
                        nextIndex[srcId] = matchIndex[srcId] + 1
                    }
                    appendEntry(srcId)
                } else {
                    nextIndex[srcId] = message.lastIndex + 1
                    matchIndex[srcId] = message.lastIndex

                    matchIndex[env.processId] = -1
                    matchIndex[0] = -1
                    while (true) {
                        val indices = matchIndex.sorted()
                        var n: Int? = null
                        for (i in indices) {
                            if (i > commitIndex && storage.readLog(i)!!.id.term == persistentState.currentTerm) {
                                n = i
                                break
                            }
                        }

                        if (n != null && matchIndex.filter { it >= n }.size >= (env.nProcesses) / 2) {
                            commitIndex = n
                        } else {
                            break
                        }
                    }
                    initial()
                    appendEntry(srcId)
                }
            }
            is AppendEntryRpc -> {
                if (message.term < persistentState.currentTerm) {
                    env.send(srcId, AppendEntryResult(persistentState.currentTerm, null))
                    return
                }
                if (state == State.LEADER) {
                    env.send(srcId, AppendEntryResult(persistentState.currentTerm, null))
                    return
                }
                if (state == State.CANDIDATE && newTerm) {
                    state = State.FOLLOWER
                }
                leader = srcId
                initial()
                val prevLogId = storage.readLog(message.prevLogId.index)?.id ?: START_LOG_ID
                if (prevLogId.term != message.prevLogId.term) {
                    env.send(srcId, AppendEntryResult(persistentState.currentTerm, null))
                    return
                }
                if (message.entry != null) {
                    env.send(
                        srcId,
                        AppendEntryResult(
                            persistentState.currentTerm,
                            message.entry.id.index
                        )
                    )
                    storage.appendLogEntry(message.entry)
                } else {
                    env.send(
                        srcId,
                        AppendEntryResult(
                            persistentState.currentTerm,
                            message.prevLogId.index
                        )
                    )
                }
                if (message.leaderCommit > commitIndex) {
                    commitIndex = min(
                        message.leaderCommit,
                        storage.readLastLogId().index,
                    )
                }
                env.startTimeout(Timeout.ELECTION_TIMEOUT)
            }
            is ClientCommandResult -> {
                env.onClientCommandResult(message.result)
                if (state == State.FOLLOWER && leader != srcId) {
                    leader = srcId
                    if (newTerm) {
                        env.startTimeout(Timeout.ELECTION_TIMEOUT)
                    }
                }
            }
            is ClientCommandRpc -> {
                onCommand(message.command)
            }
            is RequestVoteResult -> if (state == State.CANDIDATE) {
                if (persistentState.currentTerm == message.term && message.voteGranted) {
                    votes += 1
                }
                if (votes > env.nProcesses / 2) {
                    state = State.LEADER
                    initLeader()
                    heartBeat(persistentState)
                    env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
                }
            }
            is RequestVoteRpc -> if (state == State.FOLLOWER) {
                if (message.term < persistentState.currentTerm) {
                    env.send(srcId, RequestVoteResult(persistentState.currentTerm, false))
                } else if ((persistentState.votedFor == srcId || persistentState.votedFor == null) &&
                    (message.lastLogId >= storage.readLastLogId())
                ) {
                    val votedFor = persistentState.votedFor
                    storage.writePersistentState(persistentState.copy(votedFor = srcId))
                    env.send(srcId, RequestVoteResult(persistentState.currentTerm, true))
                    if (state == State.FOLLOWER && votedFor == null) {
                        env.startTimeout(Timeout.ELECTION_TIMEOUT)
                    }
                } else {
                    env.send(srcId, RequestVoteResult(persistentState.currentTerm, false))
                }
//                if (state == State.FOLLOWER) {
//                    env.startTimeout(Timeout.ELECTION_TIMEOUT)
//                }
            } else {
                env.send(srcId, RequestVoteResult(persistentState.currentTerm, false))
            }
        }
        initial()
    }

    override fun onClientCommand(command: Command) {
        initial()
        onCommand(command)
    }
}
