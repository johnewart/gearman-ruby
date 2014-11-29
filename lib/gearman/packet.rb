module Gearman
  class Packet
    # Map from Integer representations of commands used in the network
    # protocol to more-convenient symbols.
    COMMANDS = {
        1  => :can_do,               # W->J: FUNC
        2  => :cant_do,              # W->J: FUNC
        3  => :reset_abilities,      # W->J: --
        4  => :pre_sleep,            # W->J: --
        #5 =>  (unused),             # -      -
        6  => :noop,                 # J->W: --
        7  => :submit_job,           # C->J: FUNC[0]UNIQ[0]ARGS
        8  => :job_created,          # J->C: HANDLE
        9  => :grab_job,             # W->J: --
        10 => :no_job,               # J->W: --
        11 => :job_assign,           # J->W: HANDLE[0]FUNC[0]ARG
        12 => :work_status,          # W->J/C: HANDLE[0]NUMERATOR[0]DENOMINATOR
        13 => :work_complete,        # W->J/C: HANDLE[0]RES
        14 => :work_fail,            # W->J/C: HANDLE
        15 => :get_status,           # C->J: HANDLE
        16 => :echo_req,             # ?->J: TEXT
        17 => :echo_res,             # J->?: TEXT
        18 => :submit_job_bg,        # C->J: FUNC[0]UNIQ[0]ARGS
        19 => :error,                # J->?: ERRCODE[0]ERR_TEXT
        20 => :status_res,           # C->J: HANDLE[0]KNOWN[0]RUNNING[0]NUM[0]DENOM
        21 => :submit_job_high,      # C->J: FUNC[0]UNIQ[0]ARGS
        22 => :set_client_id,        # W->J: [RANDOM_STRING_NO_WHITESPACE]
        23 => :can_do_timeout,       # W->J: FUNC[0]TIMEOUT
        24 => :all_yours,            # REQ    Worker
        25 => :work_exception,       # W->J: HANDLE[0]ARG
        26 => :option_req,           # C->J: TEXT
        27 => :option_res,           # J->C: TEXT
        28 => :work_data,            # REQ    Worker
        29 => :work_warning,         # W->J/C: HANDLE[0]MSG
        30 => :grab_job_uniq,        # REQ    Worker
        31 => :job_assign_uniq,      # RES    Worker
        32 => :submit_job_high_bg,   # C->J: FUNC[0]UNIQ[0]ARGS
        33 => :submit_job_low,       # C->J: FUNC[0]UNIQ[0]ARGS
        34 => :submit_job_low_bg,    # C->J: FUNC[0]UNIQ[0]ARGS
        35 => :submit_job_sched,     # REQ    Client
        36 => :submit_job_epoch      # C->J: FUNC[0]UNIQ[0]EPOCH[0]ARGS
    }

    # Map e.g. 'can_do' => 1
    NUMS = COMMANDS.invert

    ##
    # Construct a request packet.
    #
    # @param type_name  command type's name (see COMMANDS)
    # @param arg        optional data to pack into the command
    # @return           packet (as a string)
    def Packet.pack_request(type_name, arg='')
      type_num = NUMS[type_name.to_sym]
      raise InvalidArgsError, "Invalid type name '#{type_name}'" unless type_num
      arg = '' if not arg
      "\0REQ" + [type_num, arg.size].pack('NN') + arg
    end

  end
end