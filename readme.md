# This is Guardian Australia's parser for Australian Elections

- Install python3 dependencies using pip on a Ubuntu EC2 server
- Use [Screen](http://manpages.ubuntu.com/manpages/jammy/en/man1/screen.1.html) to initiate the python script, then detach to ensure it continues running
- Run it by running parser.py


# Screen Quick Reference

## Basic

| Description 				| Command 				|
|---------------------------------------|---------------------------------------|
| Start a new session with session name | `screen -S <session_name>`		|
| List running sessions / screens	      | `screen -ls`				|
| Attach to a running session		        | `screen -x`				|
| Attach to a running session with name	| `screen -r <session_name>`		|
| Detach a running session		          | `screen -d <session_name>`		|
| Kill a running session                | `screen -X -S [session # you want to kill] kill` |
| Accessing a screen that is already attached | `screen -r -d [session name]` |

## Escape Key

All screen commands are prefixed by an escape key, by default Ctrl-a (that's Control-a, sometimes written ^a). To send a literal Ctrl-a to the programs in screen, use Ctrl-a a. This is useful when when working with screen within screen. For example Ctrl-a a n will move screen to a new window on the screen within screen. 

## Getting Out

| Description				| Command						|
|---------------------------------------|-------------------------------------------------------|
| detach 				| `Ctrl-a d`						|
| detach and logout (quick exit) 	| `Ctrl-a D D`						|
| exit screen 				| `Ctrl-a :` quit or exit all of the programs in screen.|
| force-exit screen 			| `Ctrl-a C-\` (not recommended) 			